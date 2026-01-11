# Code

`Searching4Nums_Store.py` downloads the dataset (stream) and save it in the database table `new_num_sentences`.

`mark_with_ai_and_position.py` will get rid of the `'第'+num` inside `new_num_sentences` and extract the rest sentences, annotate the numeral tokens with position index. Then every numeral tokens will be send for LLM to judge.