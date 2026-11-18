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
__vibeke_shim_path_once() {
  precmd_functions=(${precmd_functions:#__vibeke_shim_path_once})
  [[ "$path[1]" == "$VIBEKE_SHIM_DIR" ]] || path=("$VIBEKE_SHIM_DIR" "$path[@]")
  unfunction __vibeke_shim_path_once
}
precmd_functions+=(__vibeke_shim_path_once)
