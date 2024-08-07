from setuptools import setup, find_packages

setup(
    name='pygaello-ops',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        # List your package dependencies here
    ],
    description="""A utility application to help manage queries and interactions with the Gaello X neo4j graph database.
    The purpose of this microservice is to  provide the build context the project's LLM needs to make insights. Thus, its fetches, parses, processes and hands over the
    right data corresponding the given request.""",
    author='Cephas Soga',
    author_email='sogacephas@gmail.com',
    url='https://github.com/CephasSoga/Gaello-with-Janine.git',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.12',
)

