# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
import sys
from pathlib import Path
# sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tehom._version import get_versions
revision = get_versions()['version']
del get_versions


# -- Project information -----------------------------------------------------

project = 'tehom'
copyright = '2021, Jake Stevens-Haas'
author = 'Jake Stevens-Haas'

# The full version, including alpha/beta/rc tags

release = revision


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinxcontrib.apidoc',
    'sphinx.ext.autodoc',
    'sphinx.ext.todo', #optional.  Allows inline "todo:"
    'sphinx.ext.imgmath', #optional. Allows LaTeX equations 
    'sphinx.ext.napoleon', #Allows google/numpy docstrings
    'sphinx.ext.githubpages', #Adds .nojekyll file
]

apidoc_module_dir = str(Path("../..").resolve() / project)
apidoc_output_dir = str(Path().resolve())
print(apidoc_output_dir)

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []