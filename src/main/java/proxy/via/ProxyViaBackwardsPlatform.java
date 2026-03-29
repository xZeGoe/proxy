package proxy.via;

import com.viaversion.viabackwards.api.ViaBackwardsPlatform;
import com.viaversion.viaversion.api.Via;

import java.io.File;
import java.util.logging.Logger;

public final class ProxyViaBackwardsPlatform implements ViaBackwardsPlatform {
    @Override
    public Logger getLogger() {
        return Via.getPlatform().createLogger("ViaBackwards");
    }

    @Override
    public void disable() {
        getLogger().severe("ViaBackwards disabled itself.");
    }

    @Override
    public File getDataFolder() {
        return Via.getPlatform().getDataFolder();
    }
}