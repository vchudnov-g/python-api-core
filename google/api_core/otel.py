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
def start_span(name, attributes=None, span_type=None, baggage_vars=None):
    """Starts a span if OpenTelemetry is available.

    Args:
        name (str): The name of the span.
        attributes (dict): Optional attributes to attach to the span.
        span_type (str): Optional span type (e.g., 'retry', 'operation').
        baggage_vars (dict): Optional key-value pairs to set in the OTel baggage.
            Keys will be prefixed with 'goog.gapic.'.

    Yields:
        opentelemetry.trace.Span: The started span, or None if OTel is unavailable.
    """
    if not HAS_OTEL:
        yield None
        return

    # Get the current context and add all baggage to it.
    current_ctx = context.get_current()
    if baggage_vars:
        for key, value in baggage_vars.items():
            current_ctx = baggage.set_baggage(
                f"goog.gapic.{key}", str(value), context=current_ctx
            )

    # Attach the updated context.
    token = context.attach(current_ctx)

    try:
        tracer = trace.get_tracer("google-api-core")
        final_attributes = attributes.copy() if attributes else {}
        if span_type:
            final_attributes["goog.gapic.span_type"] = span_type

        with tracer.start_as_current_span(name, attributes=final_attributes) as span:
            yield span
    finally:
        context.detach(token)

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