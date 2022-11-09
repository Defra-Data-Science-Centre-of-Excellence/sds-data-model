Spatial Data Science Data Model
===============================

Installation
------------

You can install *SDS Data Model* via pip_ from GitHub_:

.. code:: console

   $ pip install git+https://github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model

However, if you're installing within a Databricks notebook, you'll need to include your Personal Access Token (PAT):

.. code:: console

   $ pip install git+https://<YOUR_PAT>@github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model

Aim
-----

Please see the `Aim`_ for details.


Usage
-----

Please see the `Usage`_ for details.


Local Development 
-----------------

To ensure compatibility with [Databricks Runtime 10.4 LTS](https://docs.databricks.com/release-notes/runtime/10.4.html), this package was developed on a Linux machine running the `Ubuntu 20.04 LTS` operating system using `Python3.8.10`, `GDAL 3.4.3`, and `spark 3.2.1.`.

Install `Python 3.8.10` using [pyenv](https://github.com/pyenv/pyenv)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

See the `pyenv-installer`'s [Installation / Update / Uninstallation](https://github.com/pyenv/pyenv-installer#installation--update--uninstallation) instructions.

Install Python 3.8.10 globally:

.. code:: console

   pyenv install 3.8.10

Then install it locally in the repository you're using:

.. code:: console

   pyenv local 3.8.10

Install `GDAL 3.4.3`
++++++++++++++++++++

Add the [UbuntuGIS unstable Private Package Archive (PPA)](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable)
and update your package list:

.. code:: console

   sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable \
   && sudo apt-get update

Install `gdal 3.4.3`, I found I also had to install python3-gdal (even though
I'm going to use poetry to install it in a virtual environment later) to
avoid version conflicts:

.. code:: console
   
   sudo apt-get install -y gdal-bin=3.4.3+dfsg-1~focal0 \
    libgdal-dev=3.4.3+dfsg-1~focal0 \
    python3-gdal=3.4.3+dfsg-1~focal0

Verify the installation:

.. code:: console

   ogrinfo --version
   # GDAL 3.4.3, released 2022/04/22


Install `poetry 1.1.13`
++++++++++++++++++++++

See poetry's [osx / linux / bashonwindows install instructions](https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions)

Install Java
++++++++++++

Java is required for Spark to work correctly. This guide details [Java installation on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-on-ubuntu-22-04).

The required commands are:

.. code:: console

   sudo apt install default-jre
   sudo apt install default-jdk

Check that both the runtime environment (jre) and development kit (jdk) are installed:

.. code:: console

   java -version
   #openjdk version "11.0.14" 2022-01-18
   #OpenJDK Runtime Environment (build 11.0.14+9-Ubuntu-0ubuntu2)
   #OpenJDK 64-Bit Server VM (build 11.0.14+9-Ubuntu-0ubuntu2, mixed mode, sharing)

   javac -version
   #javac 11.0.14

Clone this repository
+++++++++++++++++++++

.. code:: console
   
   git clone https://github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model.git


Install dependencies using `poetry`
+++++++++++++++++++++++++++++++++++

.. code:: console

   poetry install

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
.. _Aim: https://defra-data-science-centre-of-excellence.github.io/sds-data-model/aim.html
.. _Usage: https://defra-data-science-centre-of-excellence.github.io/sds-data-model/usage.html
