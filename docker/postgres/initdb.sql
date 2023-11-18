CREATE TABLE indian_liver_patient (
    Age INTEGER,
    Gender TEXT,
    Total_Bilirubin DECIMAL,
    Direct_Bilirubin DECIMAL,
    Alkaline_Phosphotase INTEGER,
    Alamine_Aminotransferase INTEGER,
    Aspartate_Aminotransferase INTEGER,
    Total_Protiens DECIMAL,
    Albumin DECIMAL,
    Albumin_and_Globulin_Ratio DECIMAL,
    Dataset INTEGER
);

COPY indian_liver_patient FROM '/tmp/indian_liver_patient.csv' DELIMITER ',' CSV HEADER;

ALTER TABLE indian_liver_patient ADD COLUMN id SERIAL PRIMARY KEY;
