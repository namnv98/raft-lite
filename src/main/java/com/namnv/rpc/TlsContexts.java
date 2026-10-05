package com.namnv.rpc;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * Tạo SSLContext cho transport socket từ keystore PKCS12.
 * Server luôn yêu cầu client xuất trình chứng chỉ, nên chỉ node có chứng chỉ được truststore tin mới gọi được RPC.
 */
public final class TlsContexts {

    private TlsContexts() {
    }

    /**
     * @param keyStore   chứa khoá riêng và chứng chỉ của node này
     * @param trustStore chứa các chứng chỉ (hoặc CA) được phép tham gia cluster
     */
    public static SSLContext fromKeyStores(Path keyStore, Path trustStore, char[] password)
            throws IOException, GeneralSecurityException {
        var keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(load(keyStore, password), password);
        var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(load(trustStore, password));
        var context = SSLContext.getInstance("TLS");
        context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
        return context;
    }

    private static KeyStore load(Path file, char[] password) throws IOException, GeneralSecurityException {
        var store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(file)) {
            store.load(in, password);
        }
        return store;
    }
}
