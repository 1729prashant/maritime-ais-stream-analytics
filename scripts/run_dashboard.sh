#!/usr/bin/env bash

export PYTHONPATH="$(pwd)"
uv run streamlit run dashboard/app.py