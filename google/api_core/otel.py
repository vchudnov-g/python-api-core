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

try:
    from opentelemetry import trace, baggage, context
    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False

@contextlib.contextmanager
def start_span(name, attributes=None, span_type=None):
    """Starts a span if OpenTelemetry is available.

    Args:
        name (str): The name of the span.
        attributes (dict): Optional attributes to attach to the span.
        span_type (str): Optional span type (e.g., 'retry', 'operation').

    Yields:
        opentelemetry.trace.Span: The started span, or None if OTel is unavailable.
    """
    if not HAS_OTEL:
        yield None
        return

    tracer = trace.get_tracer("google-api-core")
    final_attributes = attributes.copy() if attributes else {}
    if span_type:
        final_attributes["goog.gapic.span_type"] = span_type

    with tracer.start_as_current_span(name, attributes=final_attributes) as span:
        yield span

def set_baggage(key, value):
    """Sets a value in the OpenTelemetry baggage if available.

    Args:
        key (str): The baggage key (will be prefixed with 'goog.gapic.').
        value (str): The baggage value.

    Returns:
        object: A token to detach the context, or None if OTel is unavailable.
    """
    if not HAS_OTEL:
        return None

    ctx = baggage.set_baggage(f"goog.gapic.{key}", str(value))
    return context.attach(ctx)

def get_baggage(key):
    """Retrieves a value from the OpenTelemetry baggage if available.

    Args:
        key (str): The baggage key.

    Returns:
        str: The baggage value, or None if not found or OTel is unavailable.
    """
    if not HAS_OTEL:
        return None

    return baggage.get_baggage(f"goog.gapic.{key}")

def detach_context(token):
    """Detaches the context if a token is provided.

    Args:
        token (object): The token returned by set_baggage.
    """
    if HAS_OTEL and token:
        context.detach(token)
