# Project conventions
The purpose of this document is to record the semantics and conventions agreed upon by the project contributors to standardize the coding conventions of the project. These are recommended practices but by no means strict rules.

## String formatting
* Use `f”...{}....”`
* Do not use `”...{}....”.format()`
* Only use `var = "..." + "..."` sparingly

## Comments and Docstring

We adopt the python [PEP 257](https://peps.python.org/pep-0257/) docstring convention for the Python docstrings of the project and provide some examples below. 

### Comments
It is good practice to comment code to provide information about what that block of code does. The comments should precede the code and describe that part of the code. If there is another other block of code below it without a comment, you should separate it with an empty line.

See example of single-line comment below.
```
    # Set role to role_name.
    set_role(db_conn=db_conn, role_name=role_name)
```

This is an example of code without comment preceded by code with comment.
```
    # Set role to role_name.
    set_role(db_conn=db_conn, role_name=role_name)

    create_schema()
```
Pieces of code that are self-explanatory, such as `create_schema()`, don't need to be accompanied by a comment.

See example of multi-line comment below.
```
    # Result of function that does something
    # when it is called. The function returns
    # the result in the form of a dictionary.
    result = do_something()
```

### One-line and multi-line docstrings
For consistency, use """triple double quotes""" around docstrings. Use r"""raw triple double quotes""" if you use any backlashes in your docstrings and for unicode docstrings, use u"""Unicode triple-quoted strings.""". The docstring for a function or method should summarize its behavior and document its arguments, return value(s), side effects, exceptions raised, and restrictions on when it can be called (all if applicable). See [PEP 257](https://peps.python.org/pep-0257/) for more details.


One-line docstrings describes the function with one-line. See example below. 

```
def main():
    """Run the example script."""
    example_script()
    ...
```


**Notes** 
- For functions, there is no blank line either before or after the one-liner docstring. 
- The docstring is a phrase that ends with a period. The adopted [PEP 257](https://peps.python.org/pep-0257/) convention for writing the one-line description is ("Do this","Return that"). 


Multi-line docstrings consist of a summary line like the one-liner docstring, followed by a blank line, and a more elaborate description. 

See example below.
```
def create_schema(db_conn, schema_name):
    """Create schema if not exist.
    This function creates a schema if it does not exist.
    `db_conn` grant access to the schema.

    Keyword arguments:
        db_conn (object) -- database connection.
        schema_name (str) -- schema name.
    """
    db_conn.execute("create schema if not exists {schema_name};")
   ...
```

This is an example of a multi-line docstring with a return function.
```
def model_trainer(data_matrix, model):
    """Train a model on the data_matrix.

    Keyword arguments:
        data_matrix (pd.DataFrame) -- dataset containing both features and labels together.
        model (object) -- model to use for training purposes.
    Returns:
        trained_model (object) -- model that was trained on the features and labels from the matrix.
    """

    ...
```
