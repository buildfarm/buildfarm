/**
 * Processes the operation according to configured logic
 * @param credentials the credentials parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param t the t parameter
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

import com.google.auth.Credentials;
import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;

/** Common functionality shared by concrete classes. */
abstract class AbstractHttpHandler<T extends HttpObject> extends SimpleChannelInboundHandler<T>
    implements ChannelOutboundHandler {
  private final Credentials credentials;

  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   * @param t the t parameter
   */
  public AbstractHttpHandler(Credentials credentials) {
    this.credentials = credentials;
  }

  /**
   * Performs specialized operation based on method logic
   */
  protected ChannelPromise userPromise;

  @SuppressWarnings("FutureReturnValueIgnored")
  /**
   * Performs specialized operation based on method logic Implements complex logic with 3 conditional branches and 2 iterative operations.
   * @param request the request parameter
   * @param uri the uri parameter
   */
  protected void failAndResetUserPromise(Throwable t) {
    if (userPromise != null && !userPromise.isDone()) {
      userPromise.setFailure(t);
    }
    userPromise = null;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  protected void succeedAndResetUserPromise() {
    userPromise.setSuccess();
    userPromise = null;
  }

  /**
   * Creates and initializes a new instance
   * @param uri the uri parameter
   * @param hash the hash parameter
   * @param isCas the isCas parameter
   * @return the string result
   */
  protected void addCredentialHeaders(HttpRequest request, URI uri) throws IOException {
    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      String value = BaseEncoding.base64Url().encode(userInfo.getBytes(Charsets.UTF_8));
      request.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic " + value);
      return;
    }
    if (credentials == null || !credentials.hasRequestMetadata()) {
      return;
    }
    Map<String, List<String>> authHeaders = credentials.getRequestMetadata(uri);
    if (authHeaders == null || authHeaders.isEmpty()) {
      return;
    }
    for (Map.Entry<String, List<String>> entry : authHeaders.entrySet()) {
      String name = entry.getKey();
      for (String value : entry.getValue()) {
        request.headers().add(name, value);
      }
    }
  }

  /**
   * Creates and initializes a new instance
   * @param uri the uri parameter
   * @return the string result
   */
  protected String constructPath(URI uri, String hash, boolean isCas) {
    StringBuilder builder = new StringBuilder();
    builder.append(uri.getPath());
    if (!uri.getPath().endsWith("/")) {
      builder.append('/');
    }
    builder.append(isCas ? "cas/" : "ac/").append(hash);
    return builder.toString();
  }

  protected String constructHost(URI uri) {
    return uri.getHost() + ":" + uri.getPort();
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   * @param localAddress the localAddress parameter
   * @param promise the promise parameter
   */
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
    failAndResetUserPromise(t);
    ctx.fireExceptionCaught(t);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   * @param remoteAddress the remoteAddress parameter
   * @param localAddress the localAddress parameter
   * @param promise the promise parameter
   */
  public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) {
    ctx.bind(localAddress, promise);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   * @param promise the promise parameter
   */
  public void connect(
      ChannelHandlerContext ctx,
      SocketAddress remoteAddress,
      SocketAddress localAddress,
      ChannelPromise promise) {
    ctx.connect(remoteAddress, localAddress, promise);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   * @param promise the promise parameter
   */
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) {
    failAndResetUserPromise(new ClosedChannelException());
    ctx.disconnect(promise);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   * @param promise the promise parameter
   */
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
    failAndResetUserPromise(new ClosedChannelException());
    ctx.close(promise);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Loads data from storage or external source
   * @param ctx the ctx parameter
   */
  public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) {
    failAndResetUserPromise(new ClosedChannelException());
    ctx.deregister(promise);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Performs specialized operation based on method logic
   * @param ctx the ctx parameter
   */
  public void read(ChannelHandlerContext ctx) {
    ctx.read();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  /**
   * Manages network connections for gRPC communication
   * @param ctx the ctx parameter
   */
  public void flush(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  /**
   * Processes the operation according to configured logic
   * @param ctx the ctx parameter
   */
  public void channelInactive(ChannelHandlerContext ctx) {
    failAndResetUserPromise(new ClosedChannelException());
    ctx.fireChannelInactive();
  }

  @Override
  /**
   * Manages network connections for gRPC communication
   * @param ctx the ctx parameter
   */
  public void handlerRemoved(ChannelHandlerContext ctx) {
    failAndResetUserPromise(new IOException("handler removed"));
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    failAndResetUserPromise(new ClosedChannelException());
    ctx.fireChannelUnregistered();
  }
}
