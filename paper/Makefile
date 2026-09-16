MAIN := main
PAPER_PDF := entmoot-paper.pdf

.PHONY: all watch clean distclean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex
	cp $(MAIN).pdf $(PAPER_PDF)

watch:
	latexmk -pdf -pvc -interaction=nonstopmode $(MAIN).tex

# Both clean targets name $(MAIN) explicitly. Bare latexmk cleans every default
# target in the directory, which since evolution.pdf became the tracked source
# for the website copy would delete a committed file.
clean:
	latexmk -c $(MAIN).tex

distclean:
	latexmk -C $(MAIN).tex
