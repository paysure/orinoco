import os
import sys

sys.path.insert(0, os.path.abspath("../.."))


# -- Project information -----------------------------------------------------

project = "orinoco"
copyright = "Paysure Solutions Ltd."
author = "Martin Vo"

# The full version, including alpha/beta/rc tags
release = "1.0.0"


# -- General configuration ---------------------------------------------------

extensions = ["sphinx.ext.autodoc", "m2r2", "sphinx.ext.intersphinx", "sphinx.ext.viewcode"]

templates_path = ["_templates"]

exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

html_theme = "furo"

# html_theme_options = {"sidebarwidth": 350, "body_max_width": 1000}

html_static_path = ["_static"]

source_suffix = [".rst", ".md"]
