import me.lusory.relate.gradle.DependencyVersions

plugins {
    id("io.freefair.lombok") version "6.4.1"
    `java-library`
}

group = "me.lusory"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly(group = "org.springframework.boot", name = "spring-boot-starter-data-r2dbc", version = DependencyVersions.SPRING_BOOT)
    implementation(group = "org.apache.commons", name = "commons-lang3", version = DependencyVersions.COMMONS_LANG3)
    implementation(group = "net.bytebuddy", name = "byte-buddy", version = DependencyVersions.BYTEBUDDY)
    implementation(group = "net.bytebuddy", name = "byte-buddy-agent", version = DependencyVersions.BYTEBUDDY)
    implementation(group = "io.github.classgraph", name = "classgraph", version = DependencyVersions.CLASSGRAPH)

    // R2DBC drivers

    compileOnly(group = "io.r2dbc", name = "r2dbc-h2", version = DependencyVersions.H2)
    compileOnly(group = "dev.miku", name = "r2dbc-mysql", version = DependencyVersions.MYSQL)
    compileOnly(group = "io.r2dbc", name = "r2dbc-postgresql", version = DependencyVersions.POSTGRESQL)
}