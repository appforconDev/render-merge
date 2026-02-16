const express = require("express");
const { createClient } = require("@supabase/supabase-js");
const { PDFDocument } = require("pdf-lib");

const app = express();
app.use(express.json({ limit: "200mb" }));

const PORT = process.env.PORT || 3000;
const MERGE_API_SECRET = process.env.MERGE_API_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Health check
app.get("/", (req, res) => {
  res.json({ status: "ok", service: "momentli-pdf-merge" });
});

app.post("/merge", async (req, res) => {
  // Auth check
  const secret = req.headers["x-api-secret"];
  if (!secret || secret !== MERGE_API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { storagePaths, orderId, format, orderNumber } = req.body;

  if (!storagePaths || !Array.isArray(storagePaths) || storagePaths.length === 0) {
    return res.status(400).json({ error: "storagePaths is required and must be a non-empty array" });
  }

  console.log(`Merge request: orderId=${orderId}, format=${format}, batches=${storagePaths.length}`);

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // Download all batch PDFs
    const batchBuffers = [];
    for (let i = 0; i < storagePaths.length; i++) {
      console.log(`Downloading batch ${i + 1}/${storagePaths.length}: ${storagePaths[i]}`);
      const { data, error } = await supabase.storage
        .from("print-queue")
        .download(storagePaths[i]);

      if (error || !data) {
        throw new Error(`Failed to download ${storagePaths[i]}: ${error?.message}`);
      }

      const buffer = Buffer.from(await data.arrayBuffer());
      batchBuffers.push(buffer);
      console.log(`Batch ${i + 1} downloaded: ${buffer.length} bytes`);
    }

    // Merge all PDFs
    console.log("Starting PDF merge...");
    const mergedPdf = await PDFDocument.load(batchBuffers[0]);

    for (let i = 1; i < batchBuffers.length; i++) {
      console.log(`Merging batch ${i + 1}/${batchBuffers.length}...`);
      const srcPdf = await PDFDocument.load(batchBuffers[i]);
      const pages = await mergedPdf.copyPages(srcPdf, srcPdf.getPageIndices());
      for (const page of pages) {
        mergedPdf.addPage(page);
      }
    }

    const mergedBytes = await mergedPdf.save();
    const pageCount = mergedPdf.getPageCount();
    console.log(`Merge complete: ${mergedBytes.length} bytes, ${pageCount} pages`);

    // Upload merged PDF to Storage
    const outputPath = `temp/${orderId}/merged_${format}.pdf`;
    const { error: uploadError } = await supabase.storage
      .from("print-queue")
      .upload(outputPath, mergedBytes, {
        contentType: "application/pdf",
        upsert: true,
      });

    if (uploadError) {
      throw new Error(`Failed to upload merged PDF: ${uploadError.message}`);
    }

    console.log(`Uploaded merged PDF to: ${outputPath}`);

    res.json({
      storagePath: outputPath,
      size: mergedBytes.length,
      pages: pageCount,
    });
  } catch (err) {
    console.error("Merge failed:", err);
    res.status(500).json({
      error: err.message || String(err),
    });
  }
});

app.listen(PORT, () => {
  console.log(`PDF merge service running on port ${PORT}`);
});
