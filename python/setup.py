'''
Created on Nov 21, 2019

@author: pgonzalo
'''
import setuptools

setuptools.setup(
    name ="main",
    version="1.0",
    install_requires=['google-cloud-storage'],
                      #'apache-beam @ git+ssh://git@github.com/apache/beam/archive/release-2.17.0.zip#egg=apache-beam[gcp]==2.17.0&subdirectory=sdks/python'],
    #dependency_links=['https://github.com/apache/beam/archive/release-2.17.0.zip#egg=apache-beam[gcp]==2.17.0'],
    packages=setuptools.find_packages())