# Coding Standards

## TLDR
Use very informative and explicit names for everything, however long they are, as long as one knows what it is.
And do not hesitate to comment what your code does. It might be obvious to you but not to the other contributors.
Better over-commenting than under-commenting.

## File
Naming: `very_explicit_file_name_with_underscores`
```
"""
@author: Your Name
@Description: Purpose of this file/class
"""
```

## Functions
Naming: `very_explicit_function_name_with_underscores` even if long
Functions are required to have the below comments at the beginning if they are more than 4 rows long:
```
def very_explicit_function_name_with_underscores(veryExplicitVariableName1, veryExplicitVariableName2):
    """
    Description of this function
    :param veryExplicitVariableName1: variable type + what is it
    :param veryExplicitVariableName2: variable type + what is it
    :return: variable type + what is it
    """
```

## Variables
Naming: `veryExcplicitVariableName` even if long

## Comments
Put comments inside your functions to explain what you are doing.