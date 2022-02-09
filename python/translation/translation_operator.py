from os import path
from typing import List, Dict

from argostranslate import package, translate
from loguru import logger
from mlserver import MLModel, types
from mlserver.utils import get_model_uri



class TranslationAnnotatorArgos(MLModel):
    async def load(self) -> bool:
        model_dir_path = await get_model_uri(self._settings)
        model_uri: str = path.join(
            model_dir_path,
            self._settings.parameters.extra["argos_translation_model"])
        source_language_code: str = self._settings.parameters.extra["source_language_code"]
        target_language_code: str = self._settings.parameters.extra["target_language_code"]

        logger.debug(f"Loading model from \"{model_uri}\"...")
        package.install_from_path(model_uri)
        target_language: translate.Language
        [target_language] = list(filter(
            lambda language: language.code == target_language_code, translate.get_installed_languages()))
        self.model: translate.ITranslation
        [self.model] = list(filter(
            lambda translation: translation.from_lang.code == source_language_code, target_language.translations_to))
        logger.info(f"Successfully loaded model {self.model} from \"{model_uri}\".")

        return await super().load()

    async def predict(self, payload: types.InferenceRequest) -> types.InferenceResponse:
        return types.InferenceResponse(
            model_name=self.name,
            model_version="0.0.1",
            outputs=self._translate(payload),
            id=payload.id
        )

    def _translate(self, payload: types.InferenceRequest) -> List[types.ResponseOutput]:
        outputs: List[types.ResponseOutput] = []

        for request_input in payload.inputs:
            data: Dict[str, str] = self.__get_input_data(request_input)

            translated_text: str = self.model.translate(data["text_to_translate"])
            outputs.append(types.ResponseOutput(
                name="translation_annotator",
                shape=[1],
                datatype="object",
                data={
                    "translated_text": translated_text,
                    "original_text": data["text_to_translate"]
                }
            ))

        return outputs

    @staticmethod
    def __get_input_data(request_input: types.RequestInput) -> Dict[str, str]:
        return request_input.data.__root__
