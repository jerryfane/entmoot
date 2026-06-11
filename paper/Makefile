MAIN := main

.PHONY: all watch clean distclean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex

watch:
	latexmk -pdf -pvc -interaction=nonstopmode $(MAIN).tex

clean:
	latexmk -c

distclean:
	latexmk -C
