package proxy.via;

import com.viaversion.viaversion.ViaManagerImpl;
import com.viaversion.viaversion.api.Via;
import com.viaversion.viaversion.commands.ViaCommandHandler;

import java.io.File;

public final class ProxyViaBootstrap {
    private static volatile boolean initialized;

    private ProxyViaBootstrap() {
    }

    public static synchronized void init(File dataFolder, int nativeProtocolVersion) {
        if (initialized) return;

        if (!dataFolder.exists() && !dataFolder.mkdirs()) {
            throw new IllegalStateException("Failed to create Via data folder: " + dataFolder.getAbsolutePath());
        }

        final ProxyViaPlatform platform = new ProxyViaPlatform(dataFolder);
        final ProxyViaBackwardsPlatform viaBackwards = new ProxyViaBackwardsPlatform();

        final ViaManagerImpl manager = ViaManagerImpl.builder()
                .platform(platform)
                .injector(new ProxyViaInjector(nativeProtocolVersion))
                .commandHandler(new ViaCommandHandler(false))
                .loader(new ProxyViaPlatformLoader(nativeProtocolVersion))
                .build();

        Via.init(manager);

        manager.addEnableListener(() -> viaBackwards.init(new File(platform.getDataFolder(), "viabackwards.yml")));
        manager.addPostEnableListener(viaBackwards::enable);

        platform.getConf().reload();
        manager.init();
        manager.onServerLoaded();

        initialized = true;
    }
}