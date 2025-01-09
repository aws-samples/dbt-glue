import re
import os
from typing import Any
from unittest.mock import Mock

import pytest
from jinja2 import Environment, FileSystemLoader, Template

from dbt.adapters.glue.relation import SparkRelation


class TemplateBundle:
    def __init__(self, template, context, relation):
        self.template = template
        self.context = context
        self.relation = relation


class GlueMacroTestBase:
    @pytest.fixture(autouse=True)
    def config(self, context) -> dict:
        """
        Anything you put in this dict will be returned by config in the rendered template
        """
        local_config: dict[str, Any] = {}
        context["config"].get = lambda key, default=None, **kwargs: local_config.get(key, default)
        return local_config

    @pytest.fixture(scope="class")
    def default_context(self) -> dict:
        """
        This is the default context used in all tests.
        """
        context = {
            "validation": Mock(),
            "model": Mock(),
            "exceptions": Mock(),
            "config": Mock(),
            "statement": lambda r, caller: r,
            "adapter": Mock(),
            "var": Mock(),
            "return": lambda r: r,
            "is_incremental": Mock(return_value=False),
        }
        return context

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        """
        This is a list of folders from which we look to load Glue macro templates.
        All folders are relative to the dbt/include/glue folder.
        """
        return ["macros"]

    def _get_project_root(self):
        """Get the project root directory"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while not os.path.exists(os.path.join(current_dir, "dbt")):
            parent = os.path.dirname(current_dir)
            if parent == current_dir:
                raise RuntimeError("Could not find project root")
            current_dir = parent
        return current_dir

    @pytest.fixture(scope="class")
    def glue_env(self, macro_folders_to_load) -> Environment:
        """
        The environment used for rendering Glue macros
        """
        project_root = self._get_project_root()
        search_paths = []

        for folder in macro_folders_to_load:
            path = os.path.join(project_root, "dbt", "include", "glue", folder)
            if os.path.exists(path):
                search_paths.append(path)

        if not search_paths:
            raise RuntimeError(f"No macro folders found in {search_paths}")

        return Environment(
            loader=FileSystemLoader(search_paths),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        """
        The name of the Glue template you want to test, not including the path.
        Example: "adapters.sql"
        """
        raise NotImplementedError("Must be implemented by subclasses")

    @pytest.fixture
    def template(self, template_name, default_context, glue_env) -> Template:
        """
        This creates the template you will test against.
        """
        context = default_context.copy()
        current_template = glue_env.get_template(template_name, globals=context)

        def dispatch(macro_name, macro_namespace=None, packages=None):
            if hasattr(current_template.module, f"glue__{macro_name}"):
                return getattr(current_template.module, f"glue__{macro_name}")
            else:
                return context[f"glue__{macro_name}"]

        context["adapter"].dispatch = dispatch
        return current_template

    @pytest.fixture
    def context(self, template) -> dict:
        """
        Access to the context used to render the template.
        """
        return template.globals

    @pytest.fixture(scope="class")
    def relation(self):
        """
        Dummy relation to use in tests.
        """
        data = {
            "path": {
                "database": "test_db",
                "schema": "test_schema",
                "identifier": "test_table",
            },
            "type": None,
        }
        return SparkRelation.from_dict(data)

    @pytest.fixture
    def template_bundle(self, template, context, relation):
        """
        Bundles up the compiled template, its context, and a dummy relation.
        """
        context["model"].alias = relation.identifier
        return TemplateBundle(template, context, relation)

    def run_macro_raw(self, template, name, *args):
        """
        Run the named macro from a template, and return the rendered value.
        """
        return getattr(template.module, name)(*args)

    def run_macro(self, template, name, *args):
        """
        Run the named macro from a template, and return the rendered value.
        This version strips off extra whitespace and newlines.
        """
        value = self.run_macro_raw(template, name, *args)
        return re.sub(r"\s\s+", " ", value).strip()

    def render_bundle(self, template_bundle, name, *args):
        """
        Convenience method for macros that take a relation as a first argument.
        """
        return self.run_macro(template_bundle.template, name, template_bundle.relation, *args)