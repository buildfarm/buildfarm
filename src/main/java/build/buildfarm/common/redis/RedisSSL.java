// Copyright 2024 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.redis;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public final class RedisSSL {
  private RedisSSL() {
    // do not construct this helper class.
  }

  /**
   * @brief Attach ssl factory to builder. This SSLSocketFactory only trusts specified Certificate
   *     Authorities.
   * @details Create an SSL context.
   * @param certPath Path to certificate (PEM)
   * @return A builder with the SSL context attached.
   */
  public static SSLSocketFactory createSslSocketFactory(File certPath) {
    try {
      try (InputStream rootCAInputStream = Files.newInputStream(certPath.toPath())) {
        return buildSslContext(rootCAInputStream).getSocketFactory();
      }
    } catch (IOException
        | CertificateException
        | KeyStoreException
        | NoSuchAlgorithmException
        | KeyManagementException runtimeException) {
      throw new RuntimeException(runtimeException);
    }
  }

  /**
   * Create an SSLContext with one and only one root certificate authority.
   *
   * @param inputStream to read the x509 certificate PEM
   * @return
   * @throws IOException
   * @throws CertificateException
   * @throws KeyStoreException
   * @throws NoSuchAlgorithmException
   * @throws KeyManagementException
   */
  private static SSLContext buildSslContext(InputStream inputStream)
      throws IOException,
          CertificateException,
          KeyStoreException,
          NoSuchAlgorithmException,
          KeyManagementException {
    X509Certificate cert;
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null);

    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    cert = (X509Certificate) certificateFactory.generateCertificate(inputStream);
    String alias = cert.getSubjectX500Principal().getName();
    trustStore.setCertificateEntry(alias, cert);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
    tmf.init(trustStore);
    TrustManager[] trustManagers = tmf.getTrustManagers();
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagers, null);

    return sslContext;
  }
}
