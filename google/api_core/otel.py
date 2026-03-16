# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""OpenTelemetry instrumentation helpers for google-api-core."""

import contextlib

from typing import Any, Dict, Optional, Generator, Union
from enum import Enum, auto

try:
    from opentelemetry import trace, baggage, context
    from opentelemetry.trace import SpanKind, Span
    HAS_OTEL = True
    BAGGAGE_PREFIX = "goog.gapic."

    # only to get the file path:
    import sys
    from pathlib import Path
    import inspect
    import sys
    import os

    # temporary
    import traceback

    _OTEL_LEVEL = 10 # will process messages at this level and higher
except ImportError:
    HAS_OTEL = False

import threading
import time
from functools import reduce


class SemanticAttributes(Enum):
    TRANSPORT = auto()
    SPAN_ID = auto()  # "__span_id" # auto()
    DURATION = "duration_ms"
    REPEAT = auto()
    REPEAT_COUNT = auto()  # "__repeat_count" # auto()
    PARENT_SPAN_ID = auto()  # "__parent_span_id" # auto()
    TRANSPORT_NAME = "rpc.system.name"
    RETRY_COUNT =  ("grpc.grpc.resend_count", "http.request.resend_count")
    POLLING_COUNT = ("grpc.grpc.polling_count", "http.request.polling_count")
    EXCEPTION_TYPE = ("exception_type", "exception_type")
    CLIENT_VERSION = "gcp.client.version"

class SemanticAttributeValues(Enum):
    GRPC = "grpc"
    REST = "http"
    REPEAT_RETRY = auto()
    REPEAT_POLLING = auto()
    

class ChildAttributePropagator:
    # These dicts are indexed by the ID of the span that will be reading them. The name refers as to whether this data is from that span's parents or children.
    span_child_attributes = {} # these get propagated up and possibly used by some ancestors: a list of per-child dicts
    span_parent_attributes = {} # these get propagated down to and used by all descendants: a dict
    lock = threading.Lock()
    
    @classmethod
    def add_span_child_attributes(cls, parent_span_id, new_attributes: Dict[Any, Any]):
        with cls.lock:
            child_attributes = cls.span_child_attributes.get(parent_span_id, [])
            child_attributes.append(new_attributes)
            cls.span_child_attributes[parent_span_id] = child_attributes  

    @classmethod
    def pull_attributes_for_children_of_span(cls, parent_span_id):
        with cls.lock:
            children_attributes = cls.span_child_attributes.get(parent_span_id, []) # should this error if empty?
            cls.span_child_attributes[parent_span_id] = []
        return children_attributes

    @classmethod
    def add_span_parent_attributes(cls, child_span_id, new_attributes: Dict[Any, Any]):
        with cls.lock:
            existing_attributes = cls.span_parent_attributes.get(child_span_id, {})
            updated_attributes = existing_attributes | new_attributes
            cls.span_parent_attributes[child_span_id] = updated_attributes

    @classmethod
    def get_span_parent_attributes(cls, child_span_id):
        return cls.span_parent_attributes.get(child_span_id, {})

    @classmethod
    def remove_references_to_span(cls, span_id):
        with cls.lock:
            cls.span_child_attributes.pop(span_id, None)
            cls.span_parent_attributes.pop(span_id, None)

@contextlib.contextmanager
def start_span(
    name: str,
    attributes: Optional[Dict[Union[str, SemanticAttributes], Any]] = None,  # TODO: name this specific_attributes?
    span_kind: "SpanKind" = SpanKind.INTERNAL if HAS_OTEL else None,
    baggage_vars: Optional[Dict[str, str]] = None,  # TODO: name this shared_attributes?
    o11y_level = 30,
    accumulate_child_attributes = False,  # receive from children
    propagate_attributes = False,   # send to parent
    baggage_for_children = {},  # send to children
    transport: Optional[SemanticAttributeValues] = None  # set at T4 and propagated up regardless of propagate_attributes
) -> Generator[Optional["Span"], None, None]:
    """Starts a span if OpenTelemetry is available.

    Args:
        name (str): The name of the span.
        attributes (dict): Optional attributes to attach to the span.
        span_kind (opentelemetry.trace.SpanKind): Optional span kind.
            Defaults to SpanKind.INTERNAL.
        baggage_vars (dict): Optional key-value pairs to set in the OTel baggage.
            Keys will be prefixed with 'goog.gapic.'.

    Yields:
        opentelemetry.trace.Span: The started span, or None if OTel is unavailable.
    """
    if o11y_level < _OTEL_LEVEL or not HAS_OTEL:
        yield None
        return

    # print(f"*** start_span: transport=={transport}")

    # Get the current context: the current span is the parent of the new span created below
    current_ctx = context.get_current()
    parent_span_id = trace.get_current_span(current_ctx).get_span_context().span_id
    new_parent_span_attributes = ChildAttributePropagator.get_span_parent_attributes(parent_span_id)
    new_parent_span_attributes |= baggage_for_children # Note in doc: baggage will override
    if transport:
        new_parent_span_attributes[SemanticAttributes.TRANSPORT] = transport
    transport = new_parent_span_attributes.get(SemanticAttributes.TRANSPORT, None)

    # Attach the updated context.
    token = context.attach(current_ctx)  # What does this do??

    final_attributes = attributes.copy() if attributes else {} # unneeded?

    # Experimental: trace which file
    parent = _get_caller_at_depth(2)
    grandparent = _get_caller_at_depth(3)
    final_attributes["span_start"] = " *FROM* \n".join([f"{parent['function']} @ {parent['file_name']}:{parent['line_number']}",
                                                        f"{grandparent['function']} @ {grandparent['file_name']}:{grandparent['line_number']}"
                                                        ])

    
    try:
        tracer = trace.get_tracer("google-api-core")

        with tracer.start_as_current_span(name, kind=span_kind) as new_span: # attributes=final_attributes, 
            start_time = time.perf_counter()
            new_span_context = new_span.get_span_context()
            new_span_id = new_span_context.span_id
            ChildAttributePropagator.add_span_parent_attributes(new_span_id, new_parent_span_attributes)
            try:
                yield new_span
            except Exception as exception:
                raise
            finally:            
                child_attribute_list = ChildAttributePropagator.pull_attributes_for_children_of_span(new_span_id)
                all_child_attributes = merge_maps_in_order("child_attribute_list", *child_attribute_list)
                if accumulate_child_attributes:
                    if transport:
                        all_child_attributes[SemanticAttributes.TRANSPORT] = transport
                else:
                    # maybe we can optimize this?
                    transport = transport or all_child_attributes.get(SemanticAttributes.TRANSPORT, None)
                    all_child_attributes = {SemanticAttributes.TRANSPORT:  transport}  # always propagate TRANSPORT even if nothing else
                    if not transport:
                        print(f"\n********** No transport defined at\n {final_attributes['span_start']}")
                        # raise ValueError(f"\n\n********** No transport defined at\n {final_attributes['span_start']}")
                attributes_total = merge_maps_in_order("parent+final+child", new_parent_span_attributes, final_attributes, all_child_attributes)                
                attributes_total[SemanticAttributes.SPAN_ID] = new_span_id
                attributes_total[SemanticAttributes.PARENT_SPAN_ID] = parent_span_id
                if propagate_attributes:
                    ChildAttributePropagator.add_span_child_attributes(parent_span_id, attributes_total)
                else:
                    essential_propagation = {SemanticAttributes.TRANSPORT: attributes_total[SemanticAttributes.TRANSPORT]} # simplify flow above
                    ChildAttributePropagator.add_span_child_attributes(parent_span_id, essential_propagation)
                attributes_total[SemanticAttributes.DURATION] = (time.perf_counter() - start_time) * 1000 # ms
                set_attributes_in_span(new_span, attributes_total)
                ChildAttributePropagator.remove_references_to_span(new_span_id)
                
                
    finally:
        # TODO: Maybe we copy the shared attributes here.
        #  - May want to specific which type of span this is (t2..T5) nad have a list to copy for each
        #  - Need to be careful with multiple child spans, if we need to aggregate data        
        context.detach(token)

def set_attributes_in_span(span, attributes):
    print(f"=== transport key: {SemanticAttributes.TRANSPORT_NAME.value}, transport enum value: {attributes[SemanticAttributes.TRANSPORT]}")
    span.set_attribute(SemanticAttributes.TRANSPORT_NAME.value,
                       attributes[SemanticAttributes.TRANSPORT].value if attributes[SemanticAttributes.TRANSPORT] else "(!!none!!)")
    transport = attributes.get(SemanticAttributes.TRANSPORT, None)
    # print(f"\n*** transport is {transport}; which codes as {attributes[SemanticAttributes.TRANSPORT].value if attributes[SemanticAttributes.TRANSPORT] else 'none'}\n")
    if transport is SemanticAttributeValues.GRPC:
        transport_idx = 0
    elif transport is SemanticAttributeValues.REST:
        transport_idx = 1
    else:
        transport_idx = None
        
    for semantic_attribute, value in attributes.items():
        if isinstance(semantic_attribute, SemanticAttributes):
            literal_attribute = semantic_attribute.value
            if not literal_attribute or isinstance(literal_attribute, int):
                continue
            if isinstance(literal_attribute, tuple) and len(literal_attribute) == 2:
                if False and transport_idx is None: #disable for now
                    raise ValueError(f"Unset transport '{transport}' when trying to set {semantic_attribute}:{value} ")
                literal_attribute = literal_attribute[transport_idx] if transport_idx is not None else f"no_transport_{semantic_attribute.name}"
        else:
            literal_attribute = semantic_attribute

        if literal_attribute and isinstance(literal_attribute, str):
            span.set_attribute(literal_attribute, value)
        else:
            error(f"Unknown literal attribute type {literal_attribute} for semantic attribute {semantic_attribute}")


def get_baggage(key: str) -> Optional[str]:
    """Retrieves a value from the OpenTelemetry baggage if available.

    Args:
        key (str): The baggage key.

    Returns:
        str: The baggage value, or None if not found or OTel is unavailable.
    """
    if not HAS_OTEL:
        return None

    return baggage.get_baggage(f"{BAGGAGE_PREFIX}{key}")

# List of common attribute keys. In the dictionary COMMON_ATTRIBUTES, each key is an attribute key, and each value is the baggage key linked to the value that we will use to populate that attribute value.
COMMON_ATTRIBUTES = {name: f"{BAGGAGE_PREFIX}{name}" for name in [
    "gcp.client.service",
    "gcp.client.version",
    "gcp.client.repo",
    "gcp.client.artifact",
    "status.message",
    "gcp.client.language",
    "error.type",
    "exception_type"
]}

def set_attributes_from_baggage():
    new_attributes = {}
    for attribute_key, baggage_key in COMMON_ATTRIBUTES.items():
        baggage_value = baggage.get_baggage(baggage_key)
        if baggage_value:
            new_attributes[attribute_key] = baggage_value
    return new_attributes

def add_attributes_to_span(new_attributes):
    if not HAS_OTEL:
        return
    current_span = trace.get_current_span()
    if current_span.is_recording():
        for key, value in new_attributes.items():
            current_span.set_attribute(key, value)


### Misc utilities ########################################

def _get_caller_at_depth(depth=1):
    """Returns a dict with developer-useful information about any of the frames on the stakck.

    Args:    
       depth: Which caller to return information on. 0 is the caller
               of this function; 1 (default) is the caller of the
               caller of this function, etc.

    Returns: A dictwith the function name, file name, file line number of the specified caller.

    Raises: ValueError if the depth is invalid.
    """
    try:
        # depth=-1 is THIS function
        # depth=0 is the immediate caller
        # depth=1 is the caller's caller, etc.
        frame = sys._getframe(depth+1)
        info = inspect.getframeinfo(frame)

        # Extract argument names and the local variables dictionary
        args, _, _, locals_dict = inspect.getargvalues(frame)
    
        class_name = None
        if args:
            first_arg = args[0]
            # Check if the first arguma.valueent looks like a method's 'self' or 'cls'
            if first_arg == 'self':
                class_name = locals_dict['self'].__class__.__name__
            elif first_arg == 'cls':
                class_name = locals_dict['cls'].__name__

        index = info.filename.find("api_core/")
        if index == -1:
            index = info.filename.find("google/cloud/")
        if index == -1:
            index = 0
        short_filename = info.filename[index:]
        full_function_name = f"{ f'{class_name}.' if class_name else '' }{info.function}"
        
        return {
            "function": full_function_name,
            "file_name": short_filename,
            "line_number": info.lineno
        }
    except ValueError:
        return "Depth out of range"
            
def merge_maps_in_order(label, *all_maps):
    retval = reduce(lambda one, two: {**one, **two}, all_maps) if all_maps else {}
    # print(f"\n*** merge_maps_in_order[{label}]: {all_maps}\n                    types: {[type(one_map) for one_map in all_maps]}\n                     —→ {retval}\n")
    return retval
