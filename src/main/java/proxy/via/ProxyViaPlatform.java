package proxy.via;

import com.viaversion.viaversion.platform.UserConnectionViaVersionPlatform;

import java.io.File;
import java.util.logging.Logger;

public final class ProxyViaPlatform extends UserConnectionViaVersionPlatform {
    public ProxyViaPlatform(File dataFolder) {
        super(dataFolder);
    }

    @Override
    public Logger createLogger(String name) {
        return Logger.getLogger(name);
    }

    @Override
    public boolean isProxy() {
        return true;
    }

    @Override
    public String getPlatformName() {
        return "MinestomProxy";
    }

    @Override
    public String getPlatformVersion() {
        return "dev";
    }
}