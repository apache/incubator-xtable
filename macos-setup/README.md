# macOS Setup for Apache XTable™ Development

This directory contains automated setup tools and comprehensive documentation for setting up Apache XTable™ development environment on macOS.

## 🚀 Quick Start

**One command to set up everything:**

```bash
./setup-macos.sh
```

After the script completes, reload your shell:
```bash
source ~/.zshrc
```

That's it! You're ready to develop.

---

## 📁 What's in This Directory

| File | Purpose |
|------|---------|
| **[`setup-macos.sh`](./setup-macos.sh)** | ⭐ Automated setup script - **START HERE** |
| **[`MACOS_SETUP_GUIDE.md`](./MACOS_SETUP_GUIDE.md)** | Detailed manual setup with command explanations |
| **[`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md)** | Command cheat sheet for daily development |
| `setup-macos.log` | Installation summary (auto-generated after setup) |

---

## 🎯 Choose Your Path

### Path 1: Automated Setup (Recommended)
```bash
./setup-macos.sh
```
Everything is installed and configured automatically. Takes 5-10 minutes.

### Path 2: Manual Setup
Follow step-by-step instructions in [`MACOS_SETUP_GUIDE.md`](./MACOS_SETUP_GUIDE.md)

### Path 3: Quick Command Lookup
Check [`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md) for daily dev commands

---

## ✅ What Gets Installed

| Component | Version | Purpose |
|-----------|---------|---------|
| Homebrew | Latest | Package manager for macOS |
| OpenJDK | 11 | Java Development Kit (required) |
| Maven | 3.9+ | Build automation tool |
| Docker Desktop | Latest | Container platform |

---

## 🔄 Idempotency & Safety

The setup script is **idempotent** - safe to run multiple times:

- ✅ Detects existing installations
- ✅ Skips already-installed components  
- ✅ Only installs what's missing
- ✅ Safe to re-run if something fails

**Example:** If you already have Homebrew and Java, the script will only install Maven and Docker.

---

## 🖥️ Architecture Support

Works on both Intel and Apple Silicon Macs:
- **Intel Macs** (x86_64) → Homebrew in `/usr/local`
- **Apple Silicon** (M1/M2/M3 - arm64) → Homebrew in `/opt/homebrew`

The script auto-detects your Mac type and adjusts paths accordingly.

---

## 📋 After Setup

### Verify Installation
```bash
java -version          # Should show 11.x
mvn -version           # Should show Java 11
docker --version       # Should show Docker version
```

### Build the Project
```bash
cd ..                  # Go back to project root
mvn validate           # Validate project
mvn clean package -DskipTests  # Build without tests
```

---

## 🆘 Troubleshooting

**Script fails with "Permission denied"**
```bash
chmod +x setup-macos.sh
```

**Homebrew installation requires password**  
This is normal - Homebrew needs admin access to create directories.

**Maven not using Java 11**  
Reload your shell configuration:
```bash
source ~/.zshrc
mvn -version  # Should now show Java 11
```

**Docker not starting**  
Docker Desktop can take 30-60 seconds to start. Start it manually:
```bash
open -a Docker
```

**"Command not found" after installation**  
Reload your shell:
```bash
source ~/.zshrc
```

**Re-run the setup** (it's safe!):
```bash
./setup-macos.sh
```

**Check installation log**:
```bash
cat setup-macos.log
```

For more troubleshooting, see [`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md) or [`MACOS_SETUP_GUIDE.md`](./MACOS_SETUP_GUIDE.md).

---

**Last Updated**: October 1, 2025  
**Tested On**: macOS 15.7 (Sequoia) - Intel & Apple Silicon
