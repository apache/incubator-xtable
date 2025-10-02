# macOS Setup Guide for Apache XTable™ Development

This guide documents the complete setup process for developing Apache XTable™ on macOS. All commands are listed in the order they were executed.

---

## Prerequisites Check

Before installation, we checked for existing installations of required tools.

### 1. Check Java Installation
```bash
java -version
```
**Purpose**: Verify if Java is already installed on the system.  
**Expected**: Should show Java version if installed, or error if not present.

---

### 2. Check JAVA_HOME Environment Variable
```bash
echo $JAVA_HOME
```
**Purpose**: Check if JAVA_HOME environment variable is set, which many Java tools require.  
**Expected**: Shows the path to Java installation, or empty if not set.

---

### 3. Check Maven Installation
```bash
mvn -version
```
**Purpose**: Verify if Apache Maven build tool is installed.  
**Expected**: Shows Maven version, Java version it's using, and system details if installed.

---

### 4. Check Docker Installation
```bash
docker --version
```
**Purpose**: Check if Docker is installed for containerized builds.  
**Expected**: Shows Docker version if installed.

---

### 5. Check Homebrew Installation
```bash
brew --version
```
**Purpose**: Verify if Homebrew package manager is available (needed to install other tools).  
**Expected**: Shows Homebrew version if installed.

---

## Installation Steps

### 6. Install Homebrew (Package Manager for macOS)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
**Purpose**: Install Homebrew, the de facto package manager for macOS that simplifies software installation.  
**Details**: 
- Downloads and runs the official Homebrew installation script
- Creates necessary directories in `/usr/local/`
- Sets up proper permissions
- May require your admin password
- Press RETURN when prompted to continue

**Post-Install**: Homebrew will provide instructions to add it to your PATH.

---

### 7. Add Homebrew to PATH (Current Session)
```bash
eval "$(/usr/local/bin/brew shellenv)"
```
**Purpose**: Load Homebrew into the current terminal session immediately.  
**Details**: This command sets up environment variables so you can use `brew` command right away.

---

### 8. Verify Homebrew Installation
```bash
brew --version
```
**Purpose**: Confirm Homebrew was installed successfully.  
**Expected Output**: Should show Homebrew version (e.g., "Homebrew 4.6.15").

---

### 9. Install OpenJDK 11
```bash
brew install openjdk@11
```
**Purpose**: Install Java Development Kit version 11, which is required for building Apache XTable™.  
**Details**: 
- Downloads and installs OpenJDK 11 and its dependencies
- Installs to `/usr/local/Cellar/openjdk@11/`
- Version 11 is specifically required by the project (not newer versions)
- Installation includes ~30 dependencies (fonts, graphics libraries, etc.)

---

### 10. Add OpenJDK 11 to PATH (Current Session)
```bash
export PATH="/usr/local/opt/openjdk@11/bin:$PATH"
```
**Purpose**: Add Java 11 binaries to the PATH for the current terminal session.  
**Details**: Ensures the `java` and `javac` commands use OpenJDK 11.

---

### 11. Add OpenJDK 11 to PATH (Permanent - in .zshrc)
```bash
echo 'export PATH="/usr/local/opt/openjdk@11/bin:$PATH"' >> /Users/<your-username>/.zshrc
```
**Purpose**: Make Java 11 available in all future terminal sessions.  
**Details**: 
- Adds the PATH export to your zsh configuration file
- Will be loaded automatically when you open a new terminal
- Replace `/Users/amoghnatu/` with your actual home directory path

---

### 12. Set JAVA_HOME Environment Variable (Current Session)
```bash
export JAVA_HOME="/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
```
**Purpose**: Set the JAVA_HOME variable that many Java tools require.  
**Details**: Points to the Java 11 installation directory structure.

---

### 13. Set JAVA_HOME Permanently (in .zshrc)
```bash
echo 'export JAVA_HOME="/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"' >> /Users/amoghnatu/.zshrc
```
**Purpose**: Persist JAVA_HOME setting across terminal sessions.  
**Details**: Ensures Maven and other tools can always find Java 11.

---

### 14. Create System Symlink for Java 11
```bash
sudo ln -sfn /usr/local/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
```
**Purpose**: Make Java 11 discoverable by macOS system Java wrappers.  
**Details**: 
- Requires admin password (`sudo`)
- Creates a symbolic link in the system Java directory
- Allows system tools to find and use Java 11
- The `-sfn` flags: symbolic link, force (overwrite if exists), no dereference

---

### 15. Verify Java 11 Installation
```bash
java -version
```
**Purpose**: Confirm Java 11 is properly installed and accessible.  
**Expected Output**: 
```
openjdk version "11.0.28" 2025-07-15
OpenJDK Runtime Environment Homebrew (build 11.0.28+0)
OpenJDK 64-Bit Server VM Homebrew (build 11.0.28+0, mixed mode)
```

---

### 16. Install Apache Maven
```bash
brew install maven
```
**Purpose**: Install Maven, the build automation tool used by Apache XTable™.  
**Details**: 
- Downloads and installs Maven and dependencies (including OpenJDK for latest version)
- Maven will be installed to `/usr/local/Cellar/maven/`
- Homebrew automatically adds Maven to PATH

---

### 17. Verify Maven Installation
```bash
mvn -version
```
**Purpose**: Confirm Maven is installed and using Java 11.  
**Expected Output**: Should show Maven 3.9.11 and Java version 11.0.28.  
**Important**: Check that the "Java version" line shows version 11, not a newer version.

---

### 18. Install Docker Desktop
```bash
brew install --cask docker
```
**Purpose**: Install Docker Desktop application for running containers.  
**Details**: 
- Downloads and installs Docker Desktop GUI application
- Installs to `/Applications/Docker.app`
- Creates command-line tools: docker, docker-compose, etc.
- May require admin password for linking binaries
- The `--cask` flag indicates it's a GUI application, not just a command-line tool

---

### 19. Start Docker Desktop
```bash
open -a Docker
```
**Purpose**: Launch the Docker Desktop application.  
**Details**: 
- Opens Docker.app from Applications folder
- Docker daemon will start running in the background
- May take a minute for Docker to fully start
- You'll see the Docker icon in the macOS menu bar when ready

---

## Verification Steps

### 20. Verify Complete Maven Setup
```bash
mvn -version
```
**Purpose**: Double-check that Maven is using Java 11 after all configuration.  
**Expected Output**: Should confirm Java version 11.0.28 is being used by Maven.

---

### 21. Test Docker Installation
```bash
docker run hello-world
```
**Purpose**: Verify Docker is working correctly by running a simple test container.  
**Details**: 
- Downloads the "hello-world" image if not present
- Runs a container that prints a success message
- Confirms Docker daemon, client, and image pulling all work
**Expected Output**: Message stating "Hello from Docker!" with explanation of how it worked.

---

### 22. Validate Maven Project Configuration
```bash
mvn validate
```
**Purpose**: Run Maven validation to ensure the project structure and dependencies are correct.  
**Details**: 
- Executed from the Apache XTable™ project root directory
- Downloads all project dependencies (may take several minutes first time)
- Validates POM files and project structure
- Runs enforcer rules for Maven version, Java version, and dependencies
**Expected Output**: "BUILD SUCCESS" with all modules passing validation.

---

### 23. Test Maven Compile (Optional but Recommended)
```bash
mvn compile -DskipTests
```
**Purpose**: Compile the project source code without running tests to verify full build setup.  
**Details**: 
- Compiles all Java source files
- Downloads remaining dependencies
- The `-DskipTests` flag skips test execution for faster initial setup verification
- Can take 5-10 minutes on first run due to dependency downloads

---

## System Requirements Summary

| Component | Required Version | Installation Method |
|-----------|------------------|-------------------|
| macOS | Any recent version | Pre-installed |
| Homebrew | Latest | curl install script |
| Java | OpenJDK 11 | `brew install openjdk@11` |
| Maven | 3.9+ | `brew install maven` |
| Docker | Latest | `brew install --cask docker` |

---

## Environment Variables Configured

```bash
# Java 11 in PATH
export PATH="/usr/local/opt/openjdk@11/bin:$PATH"

# Java Home for tools
export JAVA_HOME="/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
```

These should be in your `~/.zshrc` file for persistence.

---

## Common Build Commands

After setup, you can use these Maven commands:

```bash
# Validate project structure
mvn validate

# Compile source code
mvn compile

# Build without tests
mvn clean package -DskipTests

# Build with tests
mvn clean package

# Run only unit tests
mvn clean test

# Run integration tests
mvn clean verify

# Clean build artifacts
mvn clean
```

---

## Troubleshooting

### Maven using wrong Java version
If Maven shows Java 25 or another version instead of Java 11:
```bash
# Re-export JAVA_HOME
export JAVA_HOME="/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"

# Verify
mvn -version
```

### Docker not starting
If `docker` command fails:
```bash
# Make sure Docker Desktop is running
open -a Docker

# Wait 30 seconds, then test again
docker --version
```

### Homebrew not found after installation
```bash
# Re-run the shellenv command
eval "$(/usr/local/bin/brew shellenv)"

# Or restart your terminal
```

### Java not found after installation
```bash
# Re-add to PATH
export PATH="/usr/local/opt/openjdk@11/bin:$PATH"

# Or source your zshrc
source ~/.zshrc
```

---

## Notes

1. **Java Version**: The project specifically requires Java 11. Do not use newer versions (Java 17, 21, etc.) as they may cause compatibility issues.

2. **First Build**: The first Maven build will download hundreds of megabytes of dependencies. Subsequent builds will be much faster.

3. **Maven Cache**: Maven 3.9+ has build caching enabled by default. You can disable it with `-Dmaven.build.cache.enabled=false` if needed.

4. **Code Style**: The project uses Spotless and Google Java Format. Run `mvn spotless:check` to check style and `mvn spotless:apply` to auto-fix issues.

5. **Architecture**: This guide was tested on macOS x86_64 (Intel). Most steps are the same for Apple Silicon (M1/M2/M3), but Homebrew might install to `/opt/homebrew/` instead of `/usr/local/`. Adjust paths accordingly.

---

## References

- [Apache XTable™ GitHub Repository](https://github.com/apache/incubator-xtable)
- [Apache XTable™ Documentation](https://xtable.apache.org/)
- [Homebrew Documentation](https://docs.brew.sh/)
- [Maven Documentation](https://maven.apache.org/guides/)
- [OpenJDK 11 Information](https://openjdk.org/projects/jdk/11/)

---

**Last Updated**: October 1, 2025  
**macOS Version Tested**: macOS 15.7 (Sequoia)  
**Architecture**: x86_64 (Intel Mac)
