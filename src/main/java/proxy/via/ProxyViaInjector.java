package proxy.via;

import com.viaversion.viaversion.api.platform.ViaInjector;
import com.viaversion.viaversion.api.protocol.version.ProtocolVersion;
import com.viaversion.viaversion.libs.gson.JsonObject;

public final class ProxyViaInjector implements ViaInjector {
    public static final String DECODER_NAME = "proxy-via-decoder";
    public static final String ENCODER_NAME = "proxy-via-encoder";

    private final ProtocolVersion nativeVersion;

    public ProxyViaInjector(int nativeProtocolVersion) {
        this.nativeVersion = ProtocolVersion.getProtocol(nativeProtocolVersion);
    }

    @Override
    public void inject() {
    }

    @Override
    public void uninject() {
    }

    @Override
    public ProtocolVersion getServerProtocolVersion() {
        return nativeVersion;
    }

    @Override
    public String getEncoderName() {
        return ENCODER_NAME;
    }

    @Override
    public String getDecoderName() {
        return DECODER_NAME;
    }

    @Override
    public JsonObject getDump() {
        return new JsonObject();
    }
}