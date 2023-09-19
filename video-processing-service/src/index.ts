import express from 'express';
import {
  uploadProcessedVideo,
  downloadRawVideo,
  deleteRawVideo,
  deleteProcessedVideo,
  convertVideo,
  setupDirectories
} from './storage';

const app = express();
app.use(express.json());

// Process a video from Cloud Storage into 360p
app.post('/process-video', async (req, res) => {
  // Get the bucket and filename from the Cloud Pub/Sub message
  let data;
  try {
    const message = Buffer.from(req.body.message.data, 'base64').toString(
      'utf-8'
    );
    data = JSON.parse(message);
    if (!data.name) {
      throw new Error('Invalid message payload');
    }
  } catch (error) {
    console.error(error);
    if (error instanceof Error) {
      return res
        .status(400)
        .send(`Bad request: missing filename.\n${error.message}`);
    } else {
      return res.status(400).send('Bad request: missing filename.');
    }
  }

  const inputFileName = data.name;
  const outputFileName = `processed-${inputFileName}`;

  // Download the raw video from Cloud Storage
  await downloadRawVideo(inputFileName);

  // Process the video into 360p
  try {
    await convertVideo(inputFileName, outputFileName);
  } catch (err) {
    // Clean up: await in parallel
    await Promise.all([
      deleteRawVideo(inputFileName),
      deleteProcessedVideo(outputFileName)
    ]);
    if (err instanceof Error) {
      return res.status(500).send(`Processing failed: ${err.message}`);
    } else {
      return res.status(500).send('Processing failed.');
    }
  }

  // Upload the processed video to Cloud Storage
  await uploadProcessedVideo(outputFileName);
  // Clean up: await in parallel
  await Promise.all([
    deleteRawVideo(inputFileName),
    deleteProcessedVideo(outputFileName)
  ]);

  return res.status(200).send('Processing finished successfully');
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Video processing service listening at http://localhost:${PORT}`);
});
