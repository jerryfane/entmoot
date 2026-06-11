MAIN := main
PAPER_PDF := entmoot-paper.pdf

.PHONY: all watch clean distclean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex
	cp $(MAIN).pdf $(PAPER_PDF)

watch:
	latexmk -pdf -pvc -interaction=nonstopmode $(MAIN).tex

clean:
	latexmk -c

distclean:
	latexmk -C
