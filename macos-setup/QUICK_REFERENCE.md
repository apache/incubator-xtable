# Quick Reference Card - Apache XTable™ Development on macOS

## One-Command Setup

```bash
./setup-macos.sh
```

---

## Installed Versions

| Tool | Version | Location |
|------|---------|----------|
| Java | OpenJDK 11.0.28 | `/usr/local/opt/openjdk@11` or `/opt/homebrew/opt/openjdk@11` |
| Maven | 3.9.11+ | Managed by Homebrew |
| Docker | Latest | `/Applications/Docker.app` |
| Homebrew | 4.6+ | `/usr/local` or `/opt/homebrew` |

---

## Essential Commands

### Check Versions
```bash
java -version          # Should show 11.x
mvn -version           # Should show Java 11
docker --version       # Any version
```

### Environment Variables
```bash
echo $JAVA_HOME        # Should point to Java 11
echo $PATH             # Should include Java 11 bin
```

### Reload Configuration
```bash
source ~/.zshrc
```

---

## Maven Commands

```bash
# Quick validation
mvn validate

# Compile only
mvn compile

# Build without tests (fast)
mvn clean package -DskipTests

# Full build with tests
mvn clean package

# Run tests only
mvn test

# Integration tests
mvn verify

# Clean build artifacts
mvn clean

# Code style check
mvn spotless:check

# Fix code style
mvn spotless:apply
```

---

## Docker Commands

```bash
# Check Docker status
docker info

# Start Docker Desktop
open -a Docker

# Test Docker
docker run hello-world

# List running containers
docker ps

# List all containers
docker ps -a
```

---

## Troubleshooting Quick Fixes

### Maven using wrong Java version
```bash
export JAVA_HOME="/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
# Or for Apple Silicon:
export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
```

### Reload shell configuration
```bash
source ~/.zshrc
# or restart terminal
```

### Docker not running
```bash
open -a Docker
# Wait 30 seconds, then try again
```

### Re-run setup script
```bash
./setup-macos.sh
# It's idempotent - safe to run anytime
```

---

## Project Structure

```
incubator-xtable/
├── xtable-api/              # API definitions
├── xtable-core/             # Core functionality
├── xtable-hudi-support/     # Hudi integration
├── xtable-utilities/        # CLI utilities
├── xtable-service/          # REST service
└── pom.xml                  # Maven parent POM
```

---

## Common Build Issues

### Build fails with Java version error
**Solution**: Ensure `mvn -version` shows Java 11

### Dependencies download slowly
**Solution**: First build downloads ~1GB. Subsequent builds are faster.

### Tests failing
**Solution**: Skip tests initially: `mvn clean package -DskipTests`

### Style check failures
**Solution**: Run `mvn spotless:apply` before committing

---

## Important Paths (Intel Mac)

```
Java 11:      /usr/local/opt/openjdk@11/
Maven:        /usr/local/Cellar/maven/
Docker:       /Applications/Docker.app
Homebrew:     /usr/local/
Shell config: ~/.zshrc
```

## Important Paths (Apple Silicon)

```
Java 11:      /opt/homebrew/opt/openjdk@11/
Maven:        /opt/homebrew/Cellar/maven/
Docker:       /Applications/Docker.app
Homebrew:     /opt/homebrew/
Shell config: ~/.zshrc
```

---

## Shell Configuration

Your `~/.zshrc` should contain:

```bash
# Homebrew
eval "$(/usr/local/bin/brew shellenv)"  # Intel
# or
eval "$(/opt/homebrew/bin/brew shellenv)"  # Apple Silicon

# Java 11
export PATH="/usr/local/opt/openjdk@11/bin:$PATH"  # Intel
# or
export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"  # Apple Silicon

export JAVA_HOME="/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"  # Intel
# or
export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"  # Apple Silicon
```

---

## Getting Help

1. **Setup issues**: See `MACOS_SETUP_GUIDE.md`
2. **Script help**: See `SETUP_README.md`
3. **Project help**: See main `README.md`
4. **GitHub Issues**: https://github.com/apache/incubator-xtable/issues

---

## Quick Health Check

Run these to verify everything is working:

```bash
# All should pass
java -version | grep "11\."
mvn -version | grep "Java version: 11"
docker info > /dev/null 2>&1 && echo "Docker OK"

# Or run the setup script again
./setup-macos.sh
```

---

## Uninstall

```bash
brew uninstall openjdk@11 maven
brew uninstall --cask docker
# Edit ~/.zshrc to remove added lines
```

---

**Print this page for quick reference during development!**

---

Last updated: October 1, 2025
