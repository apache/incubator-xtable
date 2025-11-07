#!/usr/bin/env zsh
#
# Apache XTable™ Development Environment Setup Script for macOS
# 
# This script automates the installation and configuration of all required
# tools for developing Apache XTable™ on macOS. All steps are idempotent,
# meaning you can safely run this script multiple times.
#
# Usage: 
#   chmod +x setup-macos.sh
#   ./setup-macos.sh
#
# Requirements: macOS with zsh (default shell on macOS Catalina and later)
#

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo "${RED}[ERROR]${NC} $1"
}

print_section() {
    echo ""
    echo "${BLUE}========================================${NC}"
    echo "${BLUE}$1${NC}"
    echo "${BLUE}========================================${NC}"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Detect architecture for proper paths
if [[ $(uname -m) == 'arm64' ]]; then
    BREW_PREFIX="/opt/homebrew"
    print_info "Detected Apple Silicon (M1/M2/M3) Mac"
else
    BREW_PREFIX="/usr/local"
    print_info "Detected Intel Mac"
fi

# Detect shell configuration file
if [ -f "$HOME/.zshrc" ]; then
    SHELL_CONFIG="$HOME/.zshrc"
elif [ -f "$HOME/.zprofile" ]; then
    SHELL_CONFIG="$HOME/.zprofile"
else
    SHELL_CONFIG="$HOME/.zshrc"
    touch "$SHELL_CONFIG"
fi

print_info "Using shell configuration file: $SHELL_CONFIG"

# ============================================
# 1. Install Homebrew
# ============================================
print_section "Step 1: Installing Homebrew"

if command_exists brew; then
    print_success "Homebrew is already installed"
    brew --version
else
    print_info "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    
    # Add Homebrew to PATH for current session
    if [[ -f "${BREW_PREFIX}/bin/brew" ]]; then
        eval "$("${BREW_PREFIX}/bin/brew" shellenv)"
        print_success "Homebrew installed successfully"
    else
        print_error "Homebrew installation failed"
        exit 1
    fi
fi

# Ensure Homebrew is in PATH for current session
if ! command_exists brew; then
    if [[ -f "${BREW_PREFIX}/bin/brew" ]]; then
        eval "$("${BREW_PREFIX}/bin/brew" shellenv)"
    fi
fi

# Add Homebrew to shell config if not already present
if ! grep -q "brew shellenv" "$SHELL_CONFIG"; then
    print_info "Adding Homebrew to $SHELL_CONFIG"
    echo "" >> "$SHELL_CONFIG"
    echo '# Homebrew' >> "$SHELL_CONFIG"
    echo "eval \"\$(${BREW_PREFIX}/bin/brew shellenv)\"" >> "$SHELL_CONFIG"
fi

# ============================================
# 2. Install Java 11
# ============================================
print_section "Step 2: Installing OpenJDK 11"

if brew list openjdk@11 &>/dev/null; then
    print_success "OpenJDK 11 is already installed"
    "${BREW_PREFIX}/opt/openjdk@11/bin/java" -version
else
    print_info "Installing OpenJDK 11..."
    brew install openjdk@11
    print_success "OpenJDK 11 installed successfully"
fi

# Add Java 11 to PATH in shell config if not already present
JAVA_PATH="${BREW_PREFIX}/opt/openjdk@11/bin"
if ! grep -q "openjdk@11/bin" "$SHELL_CONFIG"; then
    print_info "Adding Java 11 to PATH in $SHELL_CONFIG"
    echo "" >> "$SHELL_CONFIG"
    echo '# Java 11' >> "$SHELL_CONFIG"
    echo "export PATH=\"${JAVA_PATH}:\$PATH\"" >> "$SHELL_CONFIG"
fi

# Add Java 11 to PATH for current session
if [[ ":$PATH:" != *":${JAVA_PATH}:"* ]]; then
    export PATH="${JAVA_PATH}:$PATH"
fi

# Set JAVA_HOME in shell config if not already present
JAVA_HOME_PATH="${BREW_PREFIX}/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
if ! grep -q "JAVA_HOME.*openjdk@11" "$SHELL_CONFIG"; then
    print_info "Setting JAVA_HOME in $SHELL_CONFIG"
    echo "export JAVA_HOME=\"${JAVA_HOME_PATH}\"" >> "$SHELL_CONFIG"
fi

# Set JAVA_HOME for current session
export JAVA_HOME="${JAVA_HOME_PATH}"

# Create system symlink for Java 11 if not already present
SYSTEM_JAVA_LINK="/Library/Java/JavaVirtualMachines/openjdk-11.jdk"
if [ ! -L "$SYSTEM_JAVA_LINK" ] || [ ! -e "$SYSTEM_JAVA_LINK" ]; then
    print_info "Creating system symlink for Java 11 (requires sudo)..."
    sudo ln -sfn "${BREW_PREFIX}/opt/openjdk@11/libexec/openjdk.jdk" "$SYSTEM_JAVA_LINK"
    print_success "System symlink created"
else
    print_success "System symlink already exists"
fi

# Verify Java 11 installation
print_info "Verifying Java 11 installation..."
if java -version 2>&1 | grep -q "11\."; then
    print_success "Java 11 is properly configured"
    java -version
else
    print_warning "Java 11 may not be the default version"
    print_info "Current Java version:"
    java -version
fi

# ============================================
# 3. Install Maven
# ============================================
print_section "Step 3: Installing Apache Maven"

if command_exists mvn; then
    print_success "Maven is already installed"
    mvn -version
else
    print_info "Installing Maven..."
    brew install maven
    print_success "Maven installed successfully"
fi

# Verify Maven is using Java 11
print_info "Verifying Maven is using Java 11..."
if mvn -version 2>&1 | grep -q "Java version: 11\."; then
    print_success "Maven is configured to use Java 11"
    mvn -version
else
    print_warning "Maven may not be using Java 11"
    print_info "Current Maven configuration:"
    mvn -version
    print_info "You may need to restart your terminal or run: source $SHELL_CONFIG"
fi

# ============================================
# 4. Install Docker Desktop
# ============================================
print_section "Step 4: Installing Docker Desktop"

if [ -d "/Applications/Docker.app" ]; then
    print_success "Docker Desktop is already installed"
    if command_exists docker; then
        docker --version
    else
        print_info "Docker Desktop installed but not running. Starting Docker..."
        open -a Docker
        print_info "Waiting for Docker to start (30 seconds)..."
        sleep 30
    fi
else
    print_info "Installing Docker Desktop..."
    brew install --cask docker
    print_success "Docker Desktop installed successfully"
    
    print_info "Starting Docker Desktop..."
    open -a Docker
    print_info "Waiting for Docker to start (30 seconds)..."
    sleep 30
fi

# Wait for Docker daemon to be ready
print_info "Checking Docker daemon status..."
DOCKER_WAIT_TIME=0
MAX_WAIT=60
while ! docker info >/dev/null 2>&1; do
    if [ $DOCKER_WAIT_TIME -ge $MAX_WAIT ]; then
        print_warning "Docker daemon did not start within ${MAX_WAIT} seconds"
        print_info "Please start Docker Desktop manually and run this script again"
        break
    fi
    sleep 5
    DOCKER_WAIT_TIME=$((DOCKER_WAIT_TIME + 5))
done

if docker info >/dev/null 2>&1; then
    print_success "Docker is running"
    docker --version
fi

# ============================================
# 5. Verify Installation
# ============================================
print_section "Step 5: Verification"

print_info "Verifying all components..."
echo ""

# Check Homebrew
if command_exists brew; then
    echo "${GREEN}✓${NC} Homebrew: $(brew --version | head -1)"
else
    echo "${RED}✗${NC} Homebrew: Not found"
fi

# Check Java
if command_exists java; then
    JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2)
    if [[ $JAVA_VERSION == 11.* ]]; then
        echo "${GREEN}✓${NC} Java: OpenJDK $JAVA_VERSION"
    else
        echo "${YELLOW}⚠${NC} Java: Version $JAVA_VERSION (expected 11.x)"
    fi
else
    echo "${RED}✗${NC} Java: Not found"
fi

# Check JAVA_HOME
if [ -n "$JAVA_HOME" ] && [ -d "$JAVA_HOME" ]; then
    echo "${GREEN}✓${NC} JAVA_HOME: $JAVA_HOME"
else
    echo "${RED}✗${NC} JAVA_HOME: Not set or invalid"
fi

# Check Maven
if command_exists mvn; then
    MVN_VERSION=$(mvn -version | head -1 | awk '{print $3}')
    MVN_JAVA=$(mvn -version | grep "Java version" | awk '{print $3}')
    if [[ $MVN_JAVA == 11.* ]]; then
        echo "${GREEN}✓${NC} Maven: $MVN_VERSION (using Java $MVN_JAVA)"
    else
        echo "${YELLOW}⚠${NC} Maven: $MVN_VERSION (using Java $MVN_JAVA, expected 11.x)"
    fi
else
    echo "${RED}✗${NC} Maven: Not found"
fi

# Check Docker
if command_exists docker && docker info >/dev/null 2>&1; then
    DOCKER_VERSION=$(docker --version | awk '{print $3}' | tr -d ',')
    echo "${GREEN}✓${NC} Docker: $DOCKER_VERSION (running)"
elif command_exists docker; then
    echo "${YELLOW}⚠${NC} Docker: Installed but not running"
else
    echo "${RED}✗${NC} Docker: Not found"
fi

# ============================================
# 6. Test Docker (Optional)
# ============================================
print_section "Step 6: Testing Docker (Optional)"

if docker info >/dev/null 2>&1; then
    print_info "Running Docker hello-world test..."
    if docker run --rm hello-world >/dev/null 2>&1; then
        print_success "Docker test passed"
    else
        print_warning "Docker test failed, but Docker is installed"
    fi
else
    print_warning "Skipping Docker test (Docker not running)"
fi

# ============================================
# Final Instructions
# ============================================
print_section "Setup Complete!"

echo ""
echo "${GREEN}All required components have been installed!${NC}"
echo ""
echo "${YELLOW}Important:${NC} To ensure all environment variables are loaded, please run:"
echo "  ${BLUE}source $SHELL_CONFIG${NC}"
echo ""
echo "Or restart your terminal."
echo ""
echo "${YELLOW}Next Steps:${NC}"
echo "  1. Restart your terminal or run: source $SHELL_CONFIG"
echo "  2. Navigate to the Apache XTable™ project directory"
echo "  3. Run: ${BLUE}mvn validate${NC} to verify project setup"
echo "  4. Run: ${BLUE}mvn clean package -DskipTests${NC} to build the project"
echo ""
echo "For detailed information, see: ${BLUE}MACOS_SETUP_GUIDE.md${NC}"
echo ""

# Write a summary to a log file
LOG_FILE="setup-macos.log"
{
    echo "Apache XTable™ Setup Log"
    echo "Date: $(date)"
    echo "macOS Version: $(sw_vers -productVersion)"
    echo "Architecture: $(uname -m)"
    echo ""
    echo "Installed Components:"
    command_exists brew && echo "- Homebrew: $(brew --version | head -1)"
    command_exists java && echo "- Java: $(java -version 2>&1 | head -1)"
    [ -n "$JAVA_HOME" ] && echo "- JAVA_HOME: $JAVA_HOME"
    command_exists mvn && echo "- Maven: $(mvn -version | head -1)"
    command_exists docker && echo "- Docker: $(docker --version)"
    echo ""
    echo "Shell Configuration: $SHELL_CONFIG"
} > "$LOG_FILE"

print_success "Setup log written to: $LOG_FILE"
