# Hello World Typhoon Extension

Intended to show the structure and serve as a template for a Typhoon Extension

## Virtualenv for development

```shell script
python3 -m venv venv_typhoon
source venv_typhoon/bin/activate
python -m pip install -r requirements   # Re-run this every time you add new requirements
```

## Upload to Pypi

Make an account at https://pypi.org/account/register/.
Edit the setup.py to include project name, desired version, description etc. https://docs.python.org/3/distutils/setupscript.html
Run `make typhoon-extension` and use the user and password you created to upload.

## Upload docs to github pages

**Coming soon...**
