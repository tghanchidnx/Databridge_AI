# Contributing to DataBridge AI

Thank you for your interest in contributing to DataBridge AI! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## How to Contribute

### Reporting Issues

- Use GitHub Issues to report bugs or request features
- Check existing issues before creating a new one
- Provide clear reproduction steps for bugs
- Include your environment details (OS, Python version, etc.)

### Pull Requests

1. **Fork the repository**
   ```bash
   git clone https://github.com/tghanchidnx/Databridge_AI.git
   cd Databridge_AI
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Set up development environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # macOS/Linux
   pip install -e ".[dev]"
   ```

4. **Make your changes**
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

5. **Run tests**
   ```bash
   pytest tests/ -v
   ```

6. **Commit with a clear message**
   ```bash
   git commit -m "feat: Add new feature description"
   ```

7. **Push and create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

### Commit Message Format

We use conventional commits:

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Adding or updating tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

### Adding New Tools

When adding MCP tools:

1. Create your tool function with proper type hints
2. Add comprehensive docstring
3. Register the tool in the appropriate `mcp_tools.py`
4. Add tests in `tests/`
5. Update `docs/MANIFEST.md` by running the `update_manifest` tool

Example:
```python
@mcp.tool()
def my_new_tool(param1: str, param2: int = 10) -> dict:
    """
    Short description of what the tool does.

    Args:
        param1: Description of param1
        param2: Description of param2 (default: 10)

    Returns:
        Dictionary with results
    """
    # Implementation
    return {"status": "success", "result": ...}
```

### Creating Plugins

For the Community Edition, you can create plugins:

1. Create a folder in `databridge-ce/plugins/your_plugin/`
2. Add `mcp_tools.py` with a `register_tools(mcp_instance)` function
3. See `databridge-ce/plugins/hello_world/` for an example

## Development Guidelines

### Code Style

- Follow PEP 8
- Use type hints for all function parameters and returns
- Maximum line length: 100 characters
- Use descriptive variable names

### Testing

- Write tests for all new functionality
- Maintain test coverage above 80%
- Use pytest fixtures for common setup

### Documentation

- Update CLAUDE.md for new tool categories
- Add docstrings to all public functions
- Update wiki pages for major features

## Project Structure

```
Databridge_AI/
├── src/                    # Core modules
│   ├── hierarchy/          # Hierarchy builder
│   ├── cortex_agent/       # Cortex AI integration
│   ├── data_catalog/       # Data catalog
│   └── ...
├── databridge-ce/          # Community Edition
│   ├── plugins/            # Plugin directory
│   └── ui/                 # Web UI
├── tests/                  # Test suite
├── docs/                   # Documentation
├── templates/              # Hierarchy templates
└── skills/                 # AI skills definitions
```

## Getting Help

- Check the [Wiki](../../wiki) for documentation
- Open a Discussion for questions
- Join our community (coming soon)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
