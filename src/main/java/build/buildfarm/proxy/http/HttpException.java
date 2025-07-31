/**
 * Performs specialized operation based on method logic
 * @return the httpresponse result
 */
// Copyright 2018 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.proxy.http;

import io.netty.handler.codec.http.HttpResponse;
import java.io.IOException;

/** An exception that propagates the http status. */
final class HttpException extends IOException {
  private final HttpResponse response;

  HttpException(HttpResponse response, String message) {
    super(message, null);
    this.response = response;
  }

  public HttpResponse response() {
    return response;
  }
}
