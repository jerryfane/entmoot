MAIN := main
MERMAID := npx -y @mermaid-js/mermaid-cli
MERMAID_CONFIG := diagrams/puppeteer-config.json
DIAGRAMS := system-stack moot-state join-flow evaluation-pipeline

.PHONY: all diagrams watch clean distclean

all:
	latexmk -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex

diagrams:
	@for name in $(DIAGRAMS); do \
		$(MERMAID) -p $(MERMAID_CONFIG) -i diagrams/$$name.mmd -o figures/$$name.pdf; \
	done

watch:
	latexmk -pdf -pvc -interaction=nonstopmode $(MAIN).tex

clean:
	latexmk -c

distclean:
	latexmk -C
