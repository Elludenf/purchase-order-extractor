import { promises as fs } from 'fs';

import 'dotenv/config';
import { VertexAI, SafetySetting, ModelParams } from '@google-cloud/vertexai';


const PROJECT_ID = 'meme-detector'; // Replace with your Google Cloud project ID
const LOCATION = 'us-central1'; // Replace with your preferred location


const vertexAI = new VertexAI({
    project: PROJECT_ID,
    location: LOCATION,
    googleAuthOptions: {
        projectId: PROJECT_ID,
        universeDomain: 'googleapis.com',
        keyFile: process.env.SERVICE_ACCOUNT_CONFIG_PATH,
    }
});

const model = 'gemini-1.5-flash-001';

const generationConfig = {
    'maxOutputTokens': 8192,
    'temperature': 0,
    'topP': 0,
};
const safetySettings = [
    {
        'category': 'HARM_CATEGORY_HATE_SPEECH',
        'threshold': 'BLOCK_MEDIUM_AND_ABOVE'
    },
    {
        'category': 'HARM_CATEGORY_DANGEROUS_CONTENT',
        'threshold': 'BLOCK_MEDIUM_AND_ABOVE'
    },
    {
        'category': 'HARM_CATEGORY_SEXUALLY_EXPLICIT',
        'threshold': 'BLOCK_MEDIUM_AND_ABOVE'
    },
    {
        'category': 'HARM_CATEGORY_HARASSMENT',
        'threshold': 'BLOCK_MEDIUM_AND_ABOVE'
    }
] as SafetySetting[];


interface Response {
    seller_name: string,
    materials: { description: string, cost: string }[],
    confidence: string
}

async function extract(path: string) {
    const pdfData = await fs.readFile(path);


    const generativeModel = vertexAI.preview.getGenerativeModel({
        model: model,
        generationConfig,
        safetySettings,
    } as ModelParams);


    const instruction = {
        text: `
         You are a document entity extraction specialist for the Reibus Company. Given a set of documents Purchases Orders (AKA Purchase Orders) from different Company (aka sellers), your task is to extract the text value of the following entities from each PDF file:
        {
        "seller_name": "",
        "materials": [{"description": "", cost:""}],
        "confidence":""
        }

        - The JSON schema must be followed during the extraction.
        - The values must only include text found in the document
        - Do not normalize any entity value.
        - If an entity is not found in the document, set the entity value to null.
        - The description of the material should be a human-readable description of the material.
        - If a document is empty or cannot be read, return an empty JSON object for that document.
        `};


    const document = {
        inlineData: {
            mimeType: 'application/pdf',
            data: pdfData.toString('base64'),
        }
    };

    const documets = []

    const req = {
        contents: [
            { role: 'user', parts: [document, instruction] }
        ],
    };

    const contentResponse = await generativeModel.generateContent(req);


    if (contentResponse?.response?.candidates && contentResponse.response.candidates.length > 0) {
        const candidate = contentResponse.response.candidates[0];
        if (candidate?.content?.parts && candidate.content.parts.length > 0) {
            const text = candidate.content.parts[0].text?.replace(/\n/g, '\n').replace('```json', '').replace('```', '');
            if (text) {
                try {
                    const parsedResponse = JSON.parse(text) as Response;
                    console.log('parsedResponse', parsedResponse);
                    return parsedResponse;
                } catch (error) {
                    console.error("Failed to parse JSON:", error);
                    console.error("Invalid JSON string:", text);
                }
            }
        }
    }
    return null;

}

async function run() {
    const response: Response[] = [];
    const root = process.env.PURCHASE_ORDERS_FOLDER || '/Users/pperez/Downloads/OneDrive_1_8-9-2024/';
    let files = await fs.readdir(root);
    for (const file of files) {
        console.log(`PRocessing file  ${file} ...`);
        let result = await extract(root + file);
        if (result !== null) {
            response.push(result);

        }
    }
    console.log(JSON.stringify(response, null, 2));

}

run()
