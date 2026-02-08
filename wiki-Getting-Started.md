# Getting Started with Databridge AI

This guide will walk you through the full installation and setup process for the Databridge AI Community Edition.

## Prerequisites

- Python 3.10 or higher
- `pip` for package installation
- `git` for cloning the repository

## Installation

There are two ways to install the framework, depending on your needs.

### Option 1: Stable Release from PyPI (Recommended for Users)

For the most stable experience, install the package directly from the Python Package Index (PyPI).

```sh
pip install databridge-ai
```

### Option 2: From Source for Development (Recommended for Contributors)

If you plan to contribute to the project, you should install it from a local clone of the repository.

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/tghanchidnx/Databridge_AI.git
    cd Databridge_AI/databridge-ce
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```sh
    # For Windows
    python -m venv .venv
    .venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install in Editable Mode:** Install the package and its development dependencies. The `-e` flag (editable) means that any changes you make to the source code will immediately be reflected when you run the application.
    ```sh
    pip install -e ".[dev]"
    ```

## Running the Workbench UI

Once installed, you can launch the Databridge AI Workbench with the following command:

```sh
# Ensure you are in the 'databridge-ce' directory if you installed from source
python run_ui.py
```

This will start the local Flask server. You can access the UI by opening your web browser and navigating to:

**http://127.0.0.1:5050**

You should be greeted by the System Dashboard. Congratulations, you are now ready to use Databridge AI!
