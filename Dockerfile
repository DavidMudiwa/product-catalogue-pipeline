# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

# Install uv by copying the static binary from Astral's official image --
# faster and more reproducible than pip-installing uv or curling the installer.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/


WORKDIR /app

# Copy dependency files first so this layer is cached whenever only your
# source code changes, not your dependencies -- avoids a full re-resolve/
# re-download on every rebuild.
COPY pyproject.toml uv.lock ./

# --frozen: fail if uv.lock is out of sync with pyproject.toml, rather than
# silently re-resolving inside the image (keeps the container's dependency
# set identical to whatever you tested locally with `uv lock`).
# --no-dev: skip any dev-only dependency group, since this is a runtime image.
RUN uv sync --frozen --no-dev

# Now copy the actual project: scraper, loader, dbt project, etc.
COPY . .

# Make `uv run <anything>` the default way commands execute in this
# container, so the same image works for the scraper, the loader, or dbt
# without needing separate images.
ENTRYPOINT ["uv", "run"]

# Default to showing dbt's version if no command is given -- makes
# `docker run <image>` alone a quick sanity check that the image built
# correctly, rather than doing nothing or erroring.
CMD ["dbt", "--version"]