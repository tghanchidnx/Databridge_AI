# How to Contribute to Databridge AI

First off, thank you for considering contributing to Databridge AI! It's people like you that make open source such a great community. We welcome any and all contributions, from documentation fixes to major new features.

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

- **Discussions:** For general questions or ideas, please start a new thread in the [Discussions](https://github.com/your-username/Databridge_AI/discussions) tab.
- **Bug Reports:** To report a bug, please open an [Issue](https://github.com/your-username/Databridge_AI/issues) using the "Bug Report" template.
- **Feature Requests:** To suggest a new feature, please open an [Issue](https://github.com/your-username/Databridge_AI/issues) using the "Feature Request" template.

## Our Branching Model

To ensure stability, we follow a `dev -> test -> prod` branching model. Understanding this is key to contributing successfully.

- `prod`: This branch contains the latest stable, public release. The code here corresponds to the version available on PyPI.
- `test`: This is a pre-release branch for staging and final quality assurance (QA). It should only contain features that are ready for release.
- `dev`: This is the primary development branch where all new features and bugfixes are merged first.

**All pull requests from the community must target the `dev` branch.**

## Your First Code Contribution

Ready to contribute code? Here is the workflow to follow:

1.  **Fork the Repository:** Start by forking the `Databridge_AI` repository to your own GitHub account.

2.  **Clone Your Fork:** Clone your forked repository to your local machine.
    ```sh
    git clone https://github.com/your-username/Databridge_AI.git
    cd Databridge_AI
    ```

3.  **Add the Upstream Remote:** Add the original repository as an "upstream" remote. This allows you to keep your fork in sync.
    ```sh
    git remote add upstream https://github.com/your-username/Databridge_AI.git
    ```

4.  **Sync the `dev` Branch:** Before starting any new work, make sure your local `dev` branch is up-to-date with the upstream `dev` branch.
    ```sh
    git checkout dev
    git pull upstream dev
    ```

5.  **Create a Feature Branch:** Create a new, descriptive branch for your changes **from the `dev` branch**.
    ```sh
    # Example: git checkout -b feature/add-new-mcp-tool dev
    git checkout -b <type>/<short-description> dev
    ```

6.  **Set Up Your Environment:** Install the required dependencies.
    ```sh
    pip install -r requirements.txt
    ```

7.  **Make Your Changes:** Write your code, add your tests, and make sure all tests pass.

8.  **Commit Your Changes:** Commit your work with a clear, descriptive commit message.
    ```sh
    git add .
    git commit -m "feat: Add some amazing new feature"
    ```

9.  **Push to Your Fork:** Push your feature branch to your forked repository on GitHub.
    ```sh
    git push origin <type>/<short-description>
    ```

10. **Open a Pull Request:** Go to your fork on GitHub and click the "Contribute" button to open a new pull request.
    - Ensure the target branch is `dev`.
    - Fill out the pull request template with details about your changes.

Thank you again for your contribution!
