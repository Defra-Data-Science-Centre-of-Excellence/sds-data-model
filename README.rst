Spatial Data Science Data Model
===============================

Aim
---

In spatial data science, we want data that is easy to use.
We consider data that is easy to use as aligned to the `FAIR guiding principles <https://www.go-fair.org/fair-principles/>`_.
Wider use of FAIR in government (e.g. from the `Geospatial Commission <https://geospatialcommission.blog.gov.uk/2021/06/25/byte-ing-back-better-introducing-a-q-fair-approach-to-geospatial-data-improvement/>`_) 
has emphasised the need to ensure data are of appropriate quality and are fit for purpose, resulting in Q-FAIR:.  

**Q**\ uality

We want data that is fit for purpose. As purposes vary, we need to be able to judge data quality.
In part this means understanding the completeness, uniqueness, consistency, timeliness, validity and accuracy of data.
Different purposes will need different combinations or levels of these data dimensions.

**F**\ indable

We want data that has clear provenance - we understand where
it has come from and its subsequent transformation history. This means capturing metadata
and logging transformations in a standard way.
This will aid discovery and allow users to unambiguously document their data.

**A**\ ccessible

We want consolidated data that can be accessed centrally.
We want to work with free and open source data as much as possible, eliminating restrictions to use.
However, where licensing is the reality, we want to be clear about who can use data and restrictions to sharing.

**I**\ nteroperable

We want data that works together.
This means that we can easily combine different datasets because they share a common coordinate reference system,
spatial extent and resolution.
It should also be possible to understand the relative uncertainty in different datasets through the metadata available.

**R**\ eusable

We want data that is easy to use and reuse across different projects.
This means that data pipelines are well described and transparent such that data can be used in multiple settings.
It also means that the data are useful and relevant to different activities, forming a common foundation for analysis.

The aim of the data model is to provide the tooling that enables easy to use *analysis ready* data.
The data model is a library of code that can help us organise and standardise data using reproducible pipelines.
Reproducible pipelines, implemented in DataBricks notebooks and stored in github, will openly and transparently
document data provenance and log transformation history.

**Logging for Data Quality Considerations**

It is important that all transformations that data undergo are captured. However, it is also the case that some 
transformations will be trivial. A subset of transformations should be identified that materially affect the
resultant quality of data. For instance, the transformation of vector data with a stated 1 metre accuracy to
a raster representation with a 10 metre cells size implies a loss of precision and as such a degradation of
data quality in the resulting data. Conversely, a raster data set with a resolution of 30 metres that is converted
to a standardised 10 metre raster has not gained in data quality, despite the higher resolution of the output.

Transformations that may have implications for data quality include: e.g. data type conversion, reclassification,
resampling, geometric transformation, coordinate transformation etc.

**Ontological Considerations**

Beyond the data model, we want to be able to label and manage outputs according to a shared ontology.
Data produced by the data model has a *meaning* to users in what it represents and how it can be used in relation
to other data and outcomes of interest. However, these semantics are likely exclusive to particular users, and may not
be common to all users. Resolving this requires that these semantics are formally described to create an explicit
specification that is common to all users, this is known as an ontology.

If a suitable ontology can be created for a data created using the data model library then that ontology can be
applied to the data outputs themselves to produce annotated data. This has two immediate benefits: first, users
can identify quickly whether a dataset is likely to meet their needs, secondly, it enhances dataset findability 
by making semantics searchable. In future, it may allow for semantically similar data to be identified based on
a knowledge graph.

The core components of the release 0.1 data model library are:

1. Data classes for non-spatial rectangular data, spatial vector data, and spatial array ('raster') data.  
2. Data classes that read and store metadata belonging to the original data source(s).  
3. A 'logging tool' that records all transformations datasets undergo, writing a transformation history.  
4. A specification ('target schema') for the spatial extent, resolution and coordinate reference system of the transformed output.  
5. The ability to write a standardised output to a known and consistent data format that includes all relevant metadata.  
6. The ability to read a data format on disk to a python object that can then be used analytically.  
7. The ability to work with 4 key data types: integers, booleans, decimal/floating point numbers, and nominal categories.
8. Sufficient documentation to allow use by the core Spatial Data Science team.  

Future functionality could include:

1. Drawing directed acyclic graphs (DAGs) to represent transformation history graphically.
2. Implementing an ordinal categorical data type.  
3. Tools for finding/indexing data created for greater discoverability.  
4. A mechanism for annotating and searching data subject to a defined ontology.  
5. Applications on restricted/sensitive data.
6. Pipeline orchestration and renewal when new data becomes available.
7. Detailed tutorial and documentation to support wider uptake and use.

Installation
------------

You can install *SDS Data Model* via pip_ from GitHub_:

.. code:: console

   $ pip install git+https://github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model


Usage
-----

Please see the `Usage`_ for details.


License
-------

Distributed under the terms of the `MIT license`_,
*SDS Data Model* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.

.. _GitHub: https://github.com/
.. _MIT license: https://opensource.org/licenses/MIT
.. _file an issue: https://github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model/issues
.. _pip: https://pip.pypa.io/
.. _Usage: https://defra-data-science-centre-of-excellence.github.io/sds-data-model/usage.html
