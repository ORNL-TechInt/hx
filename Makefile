tags: hx/*.py tests/*.py
	find . -name "*.py" | xargs etags

clean:
	find . -name "__pycache__" | xargs rm -rf
	find . -name "*.pyc" | xargs rm
	find . -name "*~" | xargs rm
