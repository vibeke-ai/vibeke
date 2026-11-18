# The pane's login shell reads this once. Its remaining startup files use the original ZDOTDIR.
if (( ${+VIBEKE_ORIGINAL_ZDOTDIR} )); then
  export ZDOTDIR="$VIBEKE_ORIGINAL_ZDOTDIR"
  unset VIBEKE_ORIGINAL_ZDOTDIR
else
  unset ZDOTDIR
fi
if [[ -r "${ZDOTDIR-$HOME}/.zshenv" ]]; then
  source "${ZDOTDIR-$HOME}/.zshenv"
fi
# A login profile or directory-change hook may put a version manager first in PATH.
if (( ! ${+functions[codex]} && ! ${+aliases[codex]} )); then
  function codex { "$VIBEKE_SHIM_DIR/codex" "$@"; }
fi
