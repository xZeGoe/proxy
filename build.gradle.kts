plugins {
    id("java")
}

group = "net.minestom"
version = "dev"

repositories {
    mavenLocal()
    mavenCentral()
    maven("https://repo.viaversion.com")
}

dependencies {
    implementation("net.minestom:minestom:dev")
    implementation("org.jctools:jctools-core:4.0.5")
    implementation("it.unimi.dsi:fastutil:8.5.18")

     implementation("io.netty:netty-all:4.2.12.Final")
    implementation("com.viaversion:viaversion-common:5.7.2")
    implementation("com.viaversion:viabackwards-common:5.7.2")

    implementation("com.google.guava:guava:33.2.1-jre")
    implementation("com.google.code.gson:gson:2.11.0")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}