# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import importlib.util
import sys

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'PSync'
copyright = '2018-2026, Named Data Networking Project'
author = 'Named Data Networking Project'

# The short X.Y version.
#version = ''

# The full version, including alpha/beta/rc tags.
#release = ''

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
today_fmt = '%Y-%m-%d'


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

needs_sphinx = '7.0'
extensions = [
    'sphinx.ext.extlinks',
    'sphinx.ext.todo',
]

def addExtensionIfExists(extension: str):
    try:
        if importlib.util.find_spec(extension) is None:
            raise ModuleNotFoundError(extension)
    except (ImportError, ValueError):
        sys.stderr.write(f'WARNING: Extension {extension!r} not found. '
                         'Some documentation may not build correctly.\n')
    else:
        extensions.append(extension)

addExtensionIfExists('sphinxcontrib.doxylink')

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Disable syntax highlighting of code blocks by default.
highlight_language = 'none'

# Generate warnings for all missing references.
nitpicky = True


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_logo = 'ndn-logo.svg'
html_copy_source = False
html_show_sourcelink = False

html_theme_options = {
    'light_css_variables': {
        'color-brand-primary': '#bc4010',
        'color-brand-content': '#c85000',
        'color-brand-visited': '#c85000',
    },
    'dark_css_variables': {
        'color-brand-primary': '#fd861a',
        'color-brand-content': '#f87e00',
        'color-brand-visited': '#f87e00',
    },
    'source_repository': 'https://github.com/named-data/PSync',
    'source_branch': 'master',
}

pygments_style = 'tango'
pygments_dark_style = 'material'


# -- Misc options ------------------------------------------------------------

doxylink = {
    'psync': ('PSync.tag', 'doxygen/'),
}

extlinks = {
    'issue': ('https://redmine.named-data.net/issues/%s', 'issue #%s'),
}
