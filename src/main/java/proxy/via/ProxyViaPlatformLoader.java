package proxy.via;

import com.viaversion.viaversion.api.Via;
import com.viaversion.viaversion.api.platform.ViaPlatformLoader;
import com.viaversion.viaversion.api.protocol.version.ProtocolVersion;
import com.viaversion.viaversion.api.protocol.version.VersionProvider;

public final class ProxyViaPlatformLoader implements ViaPlatformLoader {
    private final ProtocolVersion nativeVersion;

    public ProxyViaPlatformLoader(int nativeProtocolVersion) {
        this.nativeVersion = ProtocolVersion.getProtocol(nativeProtocolVersion);
    }

    @Override
    public void load() {
        Via.getManager().getProviders().use(VersionProvider.class, ignoredConnection -> nativeVersion);
    }

    @Override
    public void unload() {
    }
}