MAIN := main
EVOLUTION := evolution
PAPER_PDF := entmoot-paper.pdf

.PHONY: all paper evolution watch clean distclean

# "all" builds the main paper only. Both PDFs in this directory are TRACKED -
# paper/ is the source of truth and website/static/papers/ is generated from it
# by "npm run sync-papers" - so a default target that rebuilt evolution.pdf
# would leave a modified binary in the tree on every source task, which is how
# a local rebuild got committed over the published bytes once already.
# Rebuilding the evolution paper is therefore explicit: make evolution.
all: paper

paper:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex
	cp $(MAIN).pdf $(PAPER_PDF)

evolution:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(EVOLUTION).tex

watch:
	latexmk -pdf -pvc -interaction=nonstopmode $(MAIN).tex

# Both clean targets name their inputs explicitly. Bare latexmk cleans every
# default target in the directory, and -C would delete the committed PDFs.
clean:
	latexmk -c $(MAIN).tex $(EVOLUTION).tex

# distclean covers $(MAIN) only: latexmk -C on $(EVOLUTION) would delete
# evolution.pdf, which is tracked rather than a byproduct.
distclean:
	latexmk -C $(MAIN).tex
