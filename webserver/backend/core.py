import os

from flask import Flask, jsonify
from reflection import get_modules_in_package, package_tree, package_tree_from_path
from typhoon.settings import typhoon_directory

app = Flask(__name__)


@app.route('/typhoon-modules')
def get_typhoon_modules():
    modules = {
        'functions': get_modules_in_package('typhoon.contrib.functions'),
        'transformations': get_modules_in_package('typhoon.contrib.transformations'),
    }
    return jsonify(modules)


@app.route('/typhoon-package-trees')
def get_typhoon_package_trees():
    package_trees = {
        'functions': package_tree('typhoon.contrib.functions'),
        'transformations': package_tree('typhoon.contrib.transformations'),
    }
    return jsonify(package_trees)


@app.route('/typhoon-user-defined-modules')
def get_user_defined_modules():
    modules = {
        'functions': get_user_defined_modules(os.path.join(typhoon_directory(), 'functions')),
        'transformations': get_user_defined_modules(os.path.join(typhoon_directory(), 'transformations')),
    }
    return jsonify(modules)


@app.route('/typhoon-user-defined-package-trees')
def get_typhoon_user_defined_package_trees():
    package_trees = {
        'functions': package_tree_from_path(os.path.join(typhoon_directory(), 'functions')),
        'transformations': package_tree_from_path(os.path.join(typhoon_directory(), 'transformations')),
    }
    return jsonify(package_trees)
