# TODO: Infer Class
"""
Infer should be the key to making everything streamlined.

Grab 10-1000 data-points from an API...

Use basic tests to try to infer the data-types

numeric_count = 0
string_count = 0
if x.is_numeric():
    numeric_count += 1
    .
    .
x has an inferred type of -> int

Use ML models to do more complex inferences. I.e. Datetimes, etc. By training it on a small handful of API's it
shouldn't be hard to infer even things like nested structures inside strings..

x = "[1, 2, 3]"
infer(x) -> list of ints

Another feature will be to parse API documentation. Api's with Good docs should make this fantastically easy.
Potentially could create a universal standard?
"""