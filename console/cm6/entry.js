// The CodeMirror 6 pieces the console's editor uses, exposed as one global so
// static/editor.js needs no module loader. Build: `bun run build` here.
import { EditorState, Compartment } from "@codemirror/state";
import { EditorView, keymap, lineNumbers, placeholder, drawSelection } from "@codemirror/view";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { bracketMatching, syntaxHighlighting, indentUnit, ensureSyntaxTree } from "@codemirror/language";
import { json } from "@codemirror/lang-json";
import { closeBrackets, closeBracketsKeymap } from "@codemirror/autocomplete";
import { setDiagnostics, lintGutter } from "@codemirror/lint";
import { classHighlighter } from "@lezer/highlight";

window.CM6 = {
  EditorState, Compartment, EditorView, keymap, lineNumbers, placeholder, drawSelection,
  defaultKeymap, history, historyKeymap, bracketMatching, syntaxHighlighting, indentUnit,
  json, ensureSyntaxTree, closeBrackets, closeBracketsKeymap, setDiagnostics, lintGutter, classHighlighter,
};
