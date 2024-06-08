import os
import shutil
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import Row

from spark_apps.data_transformations.med_qa.question_answering.question_answering_transformer import (
    run
)
from tests import SPARK


COLUMNS = ["answer", "answer_idx", "meta_info", "options", "question"]

SAMPLE_DATA = [
    {
        "answer": "Ceftriaxone",
        "answer_idx": "D",
        "meta_info": "step1",
        "options": Row(
            A="Chloramphenicol",
            B="Gentamicin",
            C="Ciprofloxacin",
            D="Ceftriaxone",
            E="Trimethoprim",
        ),
        "question": "A 21-year-old sexually active male complains of fever, pain during urination, and inflammation and pain in the right knee. A culture of the joint fluid shows a bacteria that does not ferment maltose and has no polysaccharide capsule. The physician orders antibiotic therapy for the patient. The mechanism of action of action of the medication given blocks cell wall synthesis, which of the following was given?",
    },
    {
        "answer": "Cyclic vomiting syndrome",
        "answer_idx": "A",
        "meta_info": "step2&3",
        "options": Row(
            A="Cyclic vomiting syndrome",
            B="Gastroenteritis",
            C="Hypertrophic pyloric stenosis",
            D="Gastroesophageal reflux disease",
            E="Acute intermittent porphyria",
        ),
        "question": "A 5-year-old girl is brought to the emergency department by her mother because of multiple episodes of nausea and vomiting that last about 2 hours. During this period, she has had 6–8 episodes of bilious vomiting and abdominal pain. The vomiting was preceded by fatigue. The girl feels well between these episodes. She has missed several days of school and has been hospitalized 2 times during the past 6 months for dehydration due to similar episodes of vomiting and nausea. The patient has lived with her mother since her parents divorced 8 months ago. Her immunizations are up-to-date. She is at the 60th percentile for height and 30th percentile for weight. She appears emaciated. Her temperature is 36.8°C (98.8°F), pulse is 99/min, and blood pressure is 82/52 mm Hg. Examination shows dry mucous membranes. The lungs are clear to auscultation. Abdominal examination shows a soft abdomen with mild diffuse tenderness with no guarding or rebound. The remainder of the physical examination shows no abnormalities. Which of the following is the most likely diagnosis?",
    },
    {
        "answer": "Trazodone",
        "answer_idx": "E",
        "meta_info": "step1",
        "options": Row(
            A="Diazepam",
            B="St. John’s Wort",
            C="Paroxetine",
            D="Zolpidem",
            E="Trazodone",
        ),
        "question": "A 40-year-old woman presents with difficulty falling asleep, diminished appetite, and tiredness for the past 6 weeks. She says that, despite going to bed early at night, she is unable to fall asleep. She denies feeling anxious or having disturbing thoughts while in bed. Even when she manages to fall asleep, she wakes up early in the morning and is unable to fall back asleep. She says she has grown increasingly irritable and feels increasingly hopeless, and her concentration and interest at work have diminished. The patient denies thoughts of suicide or death. Because of her diminished appetite, she has lost 4 kg (8.8 lb) in the last few weeks and has started drinking a glass of wine every night instead of eating dinner. She has no significant past medical history and is not on any medications. Which of the following is the best course of treatment in this patient?",
    },
    {
        "answer": "Obtain a urine analysis and urine culture",
        "answer_idx": "C",
        "meta_info": "step2&3",
        "options": Row(
            A="Obtain an abdominal CT scan",
            B="Obtain blood cultures",
            C="Obtain a urine analysis and urine culture",
            D="Begin intravenous treatment with ceftazidime",
            E="No treatment is necessary",
        ),
        "question": "A 37-year-old female with a history of type II diabetes mellitus presents to the emergency department complaining of blood in her urine, left-sided flank pain, nausea, and fever. She also states that she has pain with urination. Vital signs include: temperature is 102 deg F (39.4 deg C), blood pressure is 114/82 mmHg, pulse is 96/min, respirations are 18, and oxygen saturation of 97% on room air. On physical examination, the patient appears uncomfortable and has tenderness on the left flank and left costovertebral angle. Which of the following is the next best step in management?",
    },
    {
        "answer": "Hypoperfusion",
        "answer_idx": "A",
        "meta_info": "step1",
        "options": Row(
            A="Hypoperfusion",
            B="Hyperglycemia",
            C="Metabolic acidosis",
            D="Hypokalemia",
            E="Hypophosphatemia",
        ),
        "question": "A 19-year-old boy presents with confusion and the inability to speak properly. The patient's mother says that, a few hours ago, she noticed a change in the way he talked and that he appeared to be in a daze. He then lost consciousness, and she managed to get him to the hospital. She is also concerned about the weight he has lost over the past few months. His blood pressure is 80/55 mm Hg, pulse is 115/min, temperature is 37.2°C (98.9°F), and respiratory rate is 18/min. On physical examination, the patient is taking rapid, deep breaths, and his breath has a fruity odor. Dry mucous membranes and dry skin are noticeable. He is unable to cooperate for a mental status examination. Results of his arterial blood gas analysis are shown.\nPco2 16 mm Hg\nHCO3–  10 mEq/L\nPo2 91 mm Hg\npH 7.1\nHis glucose level is 450 mg/dL, and his potassium level is 4.1 mEq/L. Which of the following should be treated first in this patient?",
    },
    {
        "answer": "Iron deficiency",
        "answer_idx": "C",
        "meta_info": "step2&3",
        "options": Row(
            A="Vitamin B12 deficiency",
            B="Folate deficiency",
            C="Iron deficiency",
            D="Infiltrative bone marrow process",
            E="Intravascular hemolysis",
        ),
        "question": "A 41-year-old woman presents to her primary care physician with complaints of fatigue and weakness. She denies any personal history of blood clots or bleeding problems in her past, but she says that her mother has had to be treated for breast cancer recently and is starting to wear her down. Her past medical history is significant for preeclampsia, hypertension, polycystic ovarian syndrome, and hypercholesterolemia. She currently smokes 1 pack of cigarettes per day, drinks a glass of wine per day, and currently denies any illicit drug use. Her vital signs include: temperature, 36.7°C (98.0°F); blood pressure, 126/74 mm Hg; heart rate, 111/min; and respiratory, rate 23/min. On physical examination, her pulses are bounding and irregular, complexion is pale, but breath sounds remain clear. On examination, the physician finds diffuse skin pallor and orders a complete blood count. Her laboratory data demonstrate a hematocrit of 27.1%, MCV of 79 fL, and a reticulocyte count of 2.0%. The patient is diagnosed with anemia. Which of the following represents the most likely etiology of her anemia.",
    },
    {
        "answer": "Proteasomal degradation of ubiquitinated proteins",
        "answer_idx": "E",
        "meta_info": "step1",
        "options": Row(
            A="Lysosomal degradation of endocytosed proteins",
            B="Cytochrome c-mediated activation of proteases",
            C="Lipase-mediated degradation of triglycerides",
            D="TNF-α-mediated activation of caspases",
            E="Proteasomal degradation of ubiquitinated proteins",
        ),
        "question": "A 59-year-old woman with stage IV lung cancer comes to the physician because of progressively worsening weakness in the past 3 months. She has had a 10.5-kg (23-lb) weight loss during this period. Her BMI is 16 kg/m2. She appears thin and has bilateral temporal wasting. Which of the following is the most likely primary mechanism underlying this woman's temporal muscle atrophy?",
    },
    {
        "answer": "Non-exertional heat stroke",
        "answer_idx": "C",
        "meta_info": "step2&3",
        "options": Row(
            A="Exertional heat stroke",
            B="Neuroleptic malignant syndrome",
            C="Non-exertional heat stroke",
            D="Sepsis",
            E="Septic shock",
        ),
        "question": "A 67-year-old man presents to the emergency department with a fever and altered mental status. The patient has a history of Alzheimer dementia and is typically bed bound. His son found him confused with a warm and flushed complexion thus prompting his presentation. The patient has a past medical history of dementia, diabetes, and hypertension and typically has a visiting home nurse come to administer medications. Prior to examination, he is given haloperidol and diphenhydramine as he is combative and will not allow the nurses near him. His temperature is 102.9°F (39.4°C), blood pressure is 104/64 mmHg, pulse is 170/min, respirations are 22/min, and oxygen saturation is 100% on room air. Physical exam is notable for dry and flushed skin and a confused man. There is no skin breakdown, and flexion of the patient’s neck elicits no discomfort. Laboratory studies are drawn as seen below.\n\nHemoglobin: 15 g/dL\nHematocrit: 45%\nLeukocyte count: 4,500/mm^3 with normal differential\nPlatelet count: 227,000/mm^3\n\nSerum:\nNa+: 139 mEq/L\nCl-: 100 mEq/L\nK+: 4.3 mEq/L\nHCO3-: 24 mEq/L\nBUN: 30 mg/dL\nGlucose: 97 mg/dL\nCreatinine: 1.5 mg/dL\nCa2+: 10.2 mg/dL\nAST: 12 U/L\nALT: 10 U/L\n\nUrine:\nColor: Yellow\nBacteria: Absent\nNitrites: Negative\nRed blood cells: Negative\n\nAn initial chest radiograph is unremarkable. The patient is given 3 liters of Ringer's lactate and an electric fan to cool off. Two hours later, his temperature is 99°F (37.2°C), blood pressure is 154/94 mmHg, pulse is 100/min, respirations are 17/min, and oxygen saturation is 100% on room air. The patient’s mental status is at the patient’s baseline according to the son. Which of the following is the most likely diagnosis?",
    },
    {
        "answer": "Alpha-ketoglutarate dehydrogenase",
        "answer_idx": "A",
        "meta_info": "step1",
        "options": Row(
            A="Alpha-ketoglutarate dehydrogenase",
            B="Acyl transferases",
            C="Glycogen phosphorylase",
            D="Homocysteine methyltransferase",
            E="Succinate dehydrogenase",
        ),
        "question": "A 32-year-old man presents to a mission hospital in Cambodia because he has had difficulty walking from his village to the market. He says that he has always been healthy other than occasional infections; however, over the last year he has been having numbness in his hands and feet. Furthermore, he has noticed weakness, muscle wasting, and pain in his lower extremities. The only change he can remember is that after having a poor harvest last year, he and his family have been subsisting on white rice. Physical exam reveals normal skin color and decreased deep tendon reflexes. The most likely cause of this patient's symptoms is associated with which of the following enzymatic reactions?",
    },
    {
        "answer": "Atenolol",
        "answer_idx": "A",
        "meta_info": "step1",
        "options": Row(
            A="Atenolol",
            B="Furosemide",
            C="Hydrochlorothiazide",
            D="Nifedipine",
            E="Nitroglycerin",
        ),
        "question": "A 60-year-old woman presents to her primary care physician for a wellness checkup. She has a past medical history of hypertension and was discharged from the hospital yesterday after management of a myocardial infarction. She states that sometimes she experiences exertional angina. Her temperature is 99.5°F (37.5°C), blood pressure is 147/98 mmHg, pulse is 90/min, respirations are 17/min, and oxygen saturation is 98% on room air. Physical exam is within normal limits. Which of the following is the best next step in management?",
    },
]


def test_run_question_answering_hudi() -> None:
    input_question_answering_path = "tmp/mock_question_answering"
    transformed_dataset_path = "tmp/transformed_dataset_path"
    spark: SparkSession = SparkSession(SPARK)

    df: DataFrame = spark.createDataFrame(SAMPLE_DATA)
    df.repartition(1).write.mode("overwrite").json(input_question_answering_path, lineSep="\n")

    run(spark, input_question_answering_path, transformed_dataset_path)

    df_transformed = spark.read.format("hudi").load(
        f"file:///{os.path.abspath(transformed_dataset_path)}"
    )
    assert len(df_transformed.columns) == 7
    assert df_transformed.count() == 10

    shutil.rmtree("tmp/")

