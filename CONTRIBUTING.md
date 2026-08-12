# Contributing

Thank you for your interest in contributing to this project!

## Getting Started

1. **Fork** the repository
2. **Clone** your fork locally
3. Create a **feature branch**: `git checkout -b feature/my-feature`
4. Make your changes
5. **Commit** with a descriptive message: `git commit -m "feat: add XYZ"`
6. **Push** to your fork: `git push origin feature/my-feature`
7. Open a **Pull Request**

## Development Setup

```bash
# Clone the repo
git clone <your-fork-url>
cd ride-sharing-assignment

# Create virtual environment
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt
```

## Code Style

- Follow **PEP 8** conventions
- Use **type hints** for all function signatures
- Write **docstrings** for every public function/class
- Keep functions **short and focused** (Single Responsibility Principle)
- Use the **`logging`** module instead of `print()` statements
- All configuration belongs in **`config.py`** — no magic numbers in code

## Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

| Prefix     | Usage                          |
|------------|--------------------------------|
| `feat:`    | New feature                    |
| `fix:`     | Bug fix                        |
| `docs:`    | Documentation only             |
| `refactor:`| Code change (no feature/fix)   |
| `test:`    | Adding/updating tests          |
| `chore:`   | Build process or tooling       |

## Reporting Issues

Open a GitHub Issue with:
- A clear **title** and **description**
- Steps to **reproduce** (if applicable)
- Expected vs. actual **behaviour**
- Environment details (OS, Python version, Kafka version)
