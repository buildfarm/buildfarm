/**
 * Retrieves data from distributed storage
 * @param credentials the credentials parameter
 * @return the public result
 */
/**
 * Manages network connections for gRPC communication Implements complex logic with 9 conditional branches. Includes input validation and error handling for robustness.
 * @param ctx the ctx parameter
 * @param msg the msg parameter
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

import static com.google.common.base.Preconditions.checkState;

import com.google.auth.Credentials;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.internal.StringUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** ChannelHandler for downloads. */
final class HttpDownloadHandler extends AbstractHttpHandler<HttpObject> {
  private OutputStream out;
  private boolean keepAlive = HttpVersion.HTTP_1_1.isKeepAliveDefault();
  private boolean downloadSucceeded;
  /**
   * Constructs digest from hash string and size information
   * @param request the request parameter
   * @return the httprequest result
   */
  private HttpResponse response;

  /**
   * Persists data to storage or external destination Includes input validation and error handling for robustness.
   * @param ctx the ctx parameter
   * @param msg the msg parameter
   * @param promise the promise parameter
   */
  public HttpDownloadHandler(Credentials credentials) {
    super(credentials);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (!msg.decoderResult().isSuccess()) {
      failAndClose(new IOException("Failed to parse the HTTP response."), ctx);
      return;
    }
    if (!(msg instanceof HttpResponse) && !(msg instanceof HttpContent)) {
      failAndClose(
          new IllegalArgumentException(
              "Unsupported message type: " + StringUtil.simpleClassName(msg)),
          ctx);
      return;
    }
    checkState(userPromise != null, "response before request");

    if (msg instanceof HttpResponse) {
      response = (HttpResponse) msg;
      if (!response.protocolVersion().equals(HttpVersion.HTTP_1_1)) {
        HttpException error =
            new HttpException(
                response, "HTTP version 1.1 is required, was: " + response.protocolVersion());
        failAndClose(error, ctx);
        return;
      }
      if (!HttpUtil.isContentLengthSet(response) && !HttpUtil.isTransferEncodingChunked(response)) {
        HttpException error =
            new HttpException(
                response, "Missing 'Content-Length' or 'Transfer-Encoding: chunked' header");
        failAndClose(error, ctx);
        return;
      }
      downloadSucceeded = response.status().equals(HttpResponseStatus.OK);
      if (!downloadSucceeded) {
        out = new ByteArrayOutputStream();
      }
      keepAlive = HttpUtil.isKeepAlive((HttpResponse) msg);
    }

    if (msg instanceof HttpContent) {
      checkState(response != null, "content before headers");

      ByteBuf content = ((HttpContent) msg).content();
      content.readBytes(out, content.readableBytes());
      if (msg instanceof LastHttpContent) {
        if (downloadSucceeded) {
          succeedAndReset(ctx);
        } else {
          String errorMsg = response.status() + "\n";
          errorMsg +=
              new String(
                  ((ByteArrayOutputStream) out).toByteArray(), HttpUtil.getCharset(response));
          out.close();
          HttpException error = new HttpException(response, errorMsg);
          failAndReset(error, ctx);
        }
      }
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    checkState(userPromise == null, "handler can't be shared between pipelines.");
    userPromise = promise;
    if (!(msg instanceof DownloadCommand)) {
      failAndResetUserPromise(
          new IllegalArgumentException(
              "Unsupported message type: " + StringUtil.simpleClassName(msg)));
      return;
    }
    out = ((DownloadCommand) msg).out();
    HttpRequest request = buildRequest((DownloadCommand) msg);
    addCredentialHeaders(request, ((DownloadCommand) msg).uri());
    ctx.writeAndFlush(request)
        .addListener(
            (f) -> {
              if (!f.isSuccess()) {
                failAndClose(f.cause(), ctx);
              }
            });
  }

  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   */
  private HttpRequest buildRequest(DownloadCommand request) {
    HttpRequest httpRequest =
        new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            request.downloadContent() ? HttpMethod.GET : HttpMethod.HEAD,
            constructPath(request.uri(), request.hash(), request.casDownload()));
    httpRequest.headers().set(HttpHeaderNames.HOST, constructHost(request.uri()));
    httpRequest.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    httpRequest.headers().set(HttpHeaderNames.ACCEPT, "*/*");
    return httpRequest;
  }

  /**
   * Performs specialized operation based on method logic
   * @param t the t parameter
   * @param ctx the ctx parameter
   */
  private void succeedAndReset(ChannelHandlerContext ctx) {
    try {
      succeedAndResetUserPromise();
    } finally {
      reset(ctx);
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  /**
   * Performs specialized operation based on method logic
   * @param t the t parameter
   * @param ctx the ctx parameter
   */
  private void failAndClose(Throwable t, ChannelHandlerContext ctx) {
    try {
      failAndResetUserPromise(t);
    } finally {
      ctx.close();
    }
  }

  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   */
  private void failAndReset(Throwable t, ChannelHandlerContext ctx) {
    try {
      failAndResetUserPromise(t);
    } finally {
      reset(ctx);
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void reset(ChannelHandlerContext ctx) {
    try {
      if (!keepAlive) {
        ctx.close();
      }
    } finally {
      out = null;
      keepAlive = HttpVersion.HTTP_1_1.isKeepAliveDefault();
      downloadSucceeded = false;
      response = null;
    }
  }
}
