"""Phone-only presentation, without output quotas or changes to old material."""

import re

DEFAULT_REPLY_STYLE = (
    "你正在和用户打电话，对方是听你说话，不是看文字。延续原有人格和称呼。"
    "每次回复一般一到三句，尽量不超过六十个字，只说一个重点。"
    "内容复杂时先说结论，再问对方要不要细说，按追问展开。"
    "不要列表、编号、Markdown、括号补充和表情；数字、时间、单位按口语读法说。"
    "可以自然地追问一句。用户明确要求细说时可以展开，不省略重要事实或不确定性。"
)


def sentence_count(text):
    """Punctuation sentences, not the smaller comma/60-character TTS chunks."""
    return sum(bool(part.strip()) for part in re.split(r"[。！？!?；…\n]+", text))


class PhonePlainText:
    """Incremental prose cleaner; retain words, remove presentation syntax.

    Hold only an undecided line prefix or inline markup state, not a whole
    paragraph. Hidden reasoning/protocol is filtered by Sentences BEFORE this.
    This deliberately is not a general Markdown renderer or a length limiter.
    """

    def __init__(self):
        self.prefix = ""
        self.skip_line = False
        self.label = False
        self.after_label = False
        self.target = 0
        self.bang = False
        self.underscores = ""
        self.last = ""
        self.heading = False

    def _inline(self, text):
        output = []
        for char in text:
            if self.target:
                if char == "(":
                    self.target += 1
                elif char == ")":
                    self.target -= 1
                elif char == "\n":
                    self.target = 0
                    output.append(char)
                continue
            if self.after_label:
                self.after_label = False
                if char == "(":
                    self.target = 1
                    continue
            if self.bang:
                self.bang = False
                if char != "[":
                    output.append("!")
            if char == "!":
                self.bang = True
                continue
            if char == "[":
                self.label = True
                continue
            if char == "]" and self.label:
                self.label = False
                self.after_label = True
                continue
            if char == "_":
                self.underscores += char
                continue
            if self.underscores:
                # Preserve identifier underscores; remove emphasis delimiters.
                if self.underscores == "_" and self.last.isalnum() and char.isalnum():
                    output.append("_")
                self.underscores = ""
            if char in "*`~" or (char == "#" and self.heading):
                continue
            char = " " if char == "|" else char
            output.append(char)
            self.last = char
        return "".join(output)

    def _prefix_text(self):
        text, self.prefix = self.prefix, None
        stripped = text.strip()
        # Rules/table separators and setext underlines carry no spoken words.
        if len(stripped) >= 3 and re.fullmatch(r"[\s|:+*_=\-]+", stripped):
            return "\n" if text.endswith("\n") else ""
        text = re.sub(r"^[ \t]*(?:>[ \t]*)*", "", text)
        self.heading = text.startswith("#")
        text = re.sub(r"^(?:#{1,6}[ \t]*|(?:[-+*•]|\d+[.)、])[ \t]+)", "", text)
        text = re.sub(r"^\[[ xX]\][ \t]*", "", text)
        return self._inline(text)

    def feed(self, text="", *, final=False):
        output = []
        for char in text:
            if self.skip_line:
                if char == "\n":
                    self.skip_line = False
                    self.prefix = ""
                    output.append("\n")
                continue
            if self.prefix is not None:
                self.prefix += char
                if re.match(r"^[ \t]*(?:`{3,}|~{3,})", self.prefix):
                    self.skip_line = True
                    self.prefix = ""
                    continue
                if char == "\n":
                    output.append(self._prefix_text())
                    self.prefix = ""
                elif (
                    len(self.prefix) > 64 and not re.fullmatch(r"[\s|:+*_=\-]+", self.prefix)
                ) or not re.fullmatch(r"[ \t>#*+.\-\d)(\[\]xX~`|:=_•]*", self.prefix):
                    output.append(self._prefix_text())
            else:
                output.append(self._inline(char))
                if char == "\n":
                    self.prefix = ""
        if final:
            if self.prefix is not None and not self.skip_line:
                output.append(self._prefix_text())
            if self.bang:
                output.append("!")
                self.bang = False
            self.underscores = ""
        return "".join(output)
