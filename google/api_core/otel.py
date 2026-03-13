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

from typing import Any, Dict, Optional, Generator

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


@contextlib.contextmanager
def start_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,  # TODO: name this specific_attributes?
    span_kind: "SpanKind" = SpanKind.INTERNAL if HAS_OTEL else None,
    baggage_vars: Optional[Dict[str, str]] = None,  # TODO: name this shared_attributes?
    o11y_level = 30,
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

    # Get the current context and add all baggage to it.
    current_ctx = context.get_current()
    if baggage_vars:
        for key, value in baggage_vars.items():
            current_ctx = baggage.set_baggage(
                f"{BAGGAGE_PREFIX}{key}", str(value), context=current_ctx
            )

    baggage_attributes = set_attributes_from_baggage()

    # Attach the updated context.
    token = context.attach(current_ctx)

    final_attributes = attributes.copy() if attributes else {}

    # Experimental: trace which file
    if True:
        parent = _get_caller_at_depth(2)
        grandparent = _get_caller_at_depth(3)
        final_attributes["span_start"] = " *FROM* \n".join([f"{parent['function']} @ {parent['file_name']}:{parent['line_number']}",
                                                    f"{grandparent['function']} @ {grandparent['file_name']}:{grandparent['line_number']}"
                                                    ])

        # print(f"*** span_start : {final_attributes['span_start']} ********************")
        # traceback.print_stack()
    else:
        caller_file = os.path.relpath(Path(__file__).resolve().parents[1],
                                      sys.prefix)
        frame_record = inspect.stack()[1] 
        frame = frame_record[0]
        info = inspect.getframeinfo(frame)
        line_number = info.lineno
        final_attributes["span_start"] = f"{caller_file} @ {line_number}"
    
    final_attributes |= baggage_attributes
    if baggage_vars:
        final_attributes |= baggage_vars

    try:
        tracer = trace.get_tracer("google-api-core")

        with tracer.start_as_current_span(
            name, attributes=final_attributes, kind=span_kind
        ) as span:
            yield span
    finally:
        # TODO: Maybe we copy the shared attributes here.
        #  - May want to specific which type of span this is (t2..T5) nad have a list to copy for each
        #  - Need to be careful with multiple child spans, if we need to aggregate data
        context.detach(token)


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
            # Check if the first argument looks like a method's 'self' or 'cls'
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
            
