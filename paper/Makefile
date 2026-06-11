MAIN := main
GITHUB_PDF := $(MAIN).github.pdf

.PHONY: all github-pdf watch clean distclean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex

github-pdf: all
	gs -q -dNOPAUSE -dBATCH \
		-sDEVICE=pdfwrite \
		-dCompatibilityLevel=1.4 \
		-dPDFSETTINGS=/prepress \
		-dDetectDuplicateImages=true \
		-dCompressFonts=true \
		-sOutputFile=$(GITHUB_PDF) \
		$(MAIN).pdf
	mv $(GITHUB_PDF) $(MAIN).pdf

watch:
	latexmk -pdf -pvc -interaction=nonstopmode $(MAIN).tex

clean:
	latexmk -c

distclean:
	latexmk -C
