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
except ImportError:
    HAS_OTEL = False

##  TODO(Gemini):
#     - Reread the files for both the sample apps and these repositories. I made some manual changes since the last time we chatted.
#     - Change `span_type` below to be `span_kind` of type `SpanKind`, defauilt value INTERNAL, and set it within the context manager
#     - Add type checking
#     - Change the calling sites to specify `span_kind=INTERNAL` explicitly for now., replacing the span_type they were passing in. I'll change the actual value manually as needed later.
#     - Do not delete this comment. I'll delete it manually msyelf when we're done.
@contextlib.contextmanager
def start_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
    span_kind: "SpanKind" = SpanKind.INTERNAL if HAS_OTEL else None,
    baggage_vars: Optional[Dict[str, str]] = None,
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

        with tracer.start_as_current_span(
            name, attributes=final_attributes, kind=span_kind
        ) as span:
            yield span
    finally:
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

    return baggage.get_baggage(f"goog.gapic.{key}")
