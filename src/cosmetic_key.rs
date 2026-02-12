use openaction::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use base64::{Engine as _, engine::general_purpose};
use image::{AnimationDecoder, DynamicImage, ImageFormat, GenericImageView};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct CosmeticKeySettings {
    image_data: Option<String>,
}

pub struct CosmeticKeyAction;

// Global map to track animation controllers by instance ID
lazy_static::lazy_static! {
    static ref ANIMATIONS: Arc<RwLock<HashMap<String, AnimationController>>> 
        = Arc::new(RwLock::new(HashMap::new()));
}

// Atomic cancellation flag + stop channel for graceful shutdown
struct AnimationController {
    stop_tx: tokio::sync::oneshot::Sender<()>,
    cancelled: Arc<AtomicBool>, // Synchronous kill switch
}

#[async_trait]
impl Action for CosmeticKeyAction {
    const UUID: &'static str = "me.miella.selene.action";
    type Settings = CosmeticKeySettings;

    async fn will_appear(
        &self,
        instance: &Instance,
        settings: &Self::Settings,
    ) -> OpenActionResult<()> {
        let instance_id = instance.instance_id.clone();
        log::debug!("will_appear called for instance {}", instance_id);
        
        // Critical: Stop ANY existing animation BEFORE starting new one
        stop_animation(&instance_id).await;
        
        if let Some(image_data) = &settings.image_data {
            if is_gif(image_data) {
                start_animation(instance_id, image_data.clone()).await?;
            } else if let Ok(processed) = process_image(image_data) {
                instance.set_image(Some(processed), None).await?;
            }
        }
        Ok(())
    }

    async fn will_disappear(
        &self,
        instance: &Instance,
        _settings: &Self::Settings,
    ) -> OpenActionResult<()> {
        let instance_id = instance.instance_id.clone();
        log::debug!("will_disappear called for instance {}", instance_id);
        
        // CRITICAL FIX ORDER (prevents ghosting):
        // 1. IMMEDIATELY set cancellation flag to block future renders
        cancel_animation(&instance_id).await;
        
        // 2. Brief yield to allow any in-flight set_image to complete
        //    This prevents race where animation is between flag check and set_image call
        tokio::time::sleep(Duration::from_millis(2)).await;
        
        // 3. CLEAR IMAGE SYNCHRONOUSLY before any async operations
        //    This ensures Stream Deck/OpenDeck sees cleared state BEFORE drag completes
        if let Err(e) = instance.set_image(Option::<String>::None, None).await {
            log::warn!("Failed to clear image during disappear for {}: {:?}", instance_id, e);
        }
        
        // 4. Gracefully stop animation task (flag already prevents new renders)
        stop_animation(&instance_id).await;
        
        Ok(())
    }

    async fn did_receive_settings(
        &self,
        instance: &Instance,
        settings: &Self::Settings,
    ) -> OpenActionResult<()> {
        let instance_id = instance.instance_id.clone();
        log::debug!("did_receive_settings called for instance {}", instance_id);
        
        stop_animation(&instance_id).await;
        
        if let Some(image_data) = &settings.image_data {
            if is_gif(image_data) {
                start_animation(instance_id, image_data.clone()).await?;
            } else if let Ok(processed) = process_image(image_data) {
                instance.set_image(Some(processed), None).await?;
            }
        }
        Ok(())
    }
}

// Synchronous cancellation - blocks ALL future frame renders immediately
async fn cancel_animation(instance_id: &str) {
    let animations = ANIMATIONS.read().await;
    if let Some(controller) = animations.get(instance_id) {
        controller.cancelled.store(true, Ordering::SeqCst);
        log::trace!("Cancelled animation flag set for {}", instance_id);
    }
}

async fn stop_animation(instance_id: &str) {
    let controller = {
        let mut animations = ANIMATIONS.write().await;
        animations.remove(instance_id)
    };
    
    if let Some(controller) = controller {
        // Signal graceful shutdown (task will exit on next loop iteration)
        let _ = controller.stop_tx.send(());
        
        // Short wait for cleanup (no exponential backoff needed - flag already prevents renders)
        tokio::time::sleep(Duration::from_millis(5)).await;
        log::info!("Stopped animation for instance {}", instance_id);
    } else {
        log::debug!("No animation to stop for instance {}", instance_id);
    }
}

async fn start_animation(instance_id: String, image_data: String) -> OpenActionResult<()> {
    // Quick pre-check: instance must exist before expensive processing
    if get_instance(instance_id.clone()).await.is_none() {
        log::debug!("Instance {} gone before animation start", instance_id);
        return Ok(());
    }
    
    // Process frames in blocking task
    let frames = match tokio::task::spawn_blocking(move || prepare_gif_frames(&image_data)).await {
        Ok(Ok(f)) => f,
        Ok(Err(e)) => {
            log::error!("GIF processing failed: {}", e);
            return Ok(());
        }
        Err(e) => {
            log::error!("GIF processing panicked: {:?}", e);
            return Ok(());
        }
    };
    
    if frames.is_empty() {
        log::warn!("No frames in GIF for instance {}", instance_id);
        return Ok(());
    }
    
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let cancelled = Arc::new(AtomicBool::new(false));
    
    // Store controller BEFORE spawning task (safe because we check cancelled flag immediately in task)
    let mut animations = ANIMATIONS.write().await;
    animations.insert(
        instance_id.clone(),
        AnimationController { stop_tx, cancelled: cancelled.clone() },
    );
    drop(animations);
    
    let instance_id_for_task = instance_id.clone();
    
    tokio::spawn(async move {
        // Final instance verification with retries
        let Some(mut cached_instance) = retry_get_instance(instance_id_for_task.clone(), 15, 100).await else {
            log::warn!("Failed to get instance {} for animation", instance_id_for_task);
            cleanup_animation(&instance_id_for_task).await;
            return;
        };
        
        // Pre-calculate frame timings
        let mut frame_timings = Vec::with_capacity(frames.len());
        let mut cumulative_ms = 0u64;
        for (_, delay_ms) in &frames {
            frame_timings.push(cumulative_ms);
            cumulative_ms += *delay_ms as u64;
        }
        let total_duration_ms = cumulative_ms;
        let animation_start = tokio::time::Instant::now();
        let mut stop_rx = stop_rx;
        
        loop {
            // CRITICAL: Check cancellation BEFORE any work for this frame
            if cancelled.load(Ordering::SeqCst) {
                log::debug!("Animation cancelled for {} (flag check)", instance_id_for_task);
                break;
            }
            
            // Calculate current frame
            let elapsed = animation_start.elapsed().as_millis() as u64;
            let position_in_loop = elapsed % total_duration_ms;
            let frame_index = frame_timings
                .iter()
                .position(|&t| t > position_in_loop)
                .map(|i| i.saturating_sub(1))
                .unwrap_or(frames.len() - 1);
            let (frame_data, _) = &frames[frame_index];
            
            // CRITICAL: Check cancellation AGAIN right before expensive image operation
            if cancelled.load(Ordering::SeqCst) {
                break;
            }
            
            // Additional safety: verify controller still exists (not removed by stop_animation)
            {
                let animations = ANIMATIONS.read().await;
                if !animations.contains_key(&instance_id_for_task) {
                    log::debug!("Animation controller removed for {}, stopping", instance_id_for_task);
                    break;
                }
            }
            
            // Set image with recovery
            if let Err(e) = cached_instance.set_image(Some(frame_data.clone()), None).await {
                log::warn!("Failed to set image for {}: {:?}", instance_id_for_task, e);
                if let Some(new_instance) = get_instance(instance_id_for_task.clone()).await {
                    cached_instance = new_instance;
                } else {
                    log::error!("Lost instance {} during animation", instance_id_for_task);
                    break;
                }
            }
            
            // Calculate next frame timing
            let next_frame_time = *frame_timings.get(frame_index + 1).unwrap_or(&total_duration_ms);
            let time_until_next = next_frame_time.saturating_sub(position_in_loop);
            let wait_duration = Duration::from_millis(time_until_next.max(16));
            
            // Wait with cancellation priority
            tokio::select! {
                _ = tokio::time::sleep(wait_duration) => {},
                _ = &mut stop_rx => {
                    log::debug!("Animation stopped via signal for {}", instance_id_for_task);
                    break;
                }
            }
        }
        
        cleanup_animation(&instance_id_for_task).await;
    });
    
    Ok(())
}

async fn cleanup_animation(instance_id: &str) {
    let mut animations = ANIMATIONS.write().await;
    animations.remove(instance_id);
    log::trace!("Cleaned up animation for {}", instance_id);
}

async fn retry_get_instance(instance_id: String, max_attempts: u32, delay_ms: u64) -> Option<Arc<Instance>> {
    for _attempt in 0..max_attempts {
        if let Some(instance) = get_instance(instance_id.clone()).await {
            return Some(instance);
        }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    None
}

// Rest of helper functions unchanged but with safety improvements
fn prepare_gif_frames(image_data: &str) -> Result<Vec<(String, u32)>, String> {
    let gif_bytes = decode_data_url(image_data)?;
    if gif_bytes.len() > 10 * 1024 * 1024 {
        return Err("GIF exceeds 10MB size limit".to_string());
    }
    
    let decoder = image::codecs::gif::GifDecoder::new(std::io::Cursor::new(&gif_bytes))
        .map_err(|e| format!("GIF decode failed: {}", e))?;
    
    let frames = decoder.into_frames().collect_frames()
        .map_err(|e| format!("Frame extraction failed: {}", e))?;
    
    if frames.is_empty() {
        return Err("GIF contains no frames".to_string());
    }
    
    let frame_limit = 200;
    let mut processed_frames = Vec::with_capacity(frames.len().min(frame_limit));
    
    for (_i, frame) in frames.into_iter().enumerate().take(frame_limit) {
        let delay = frame.delay();
        let (numer, denom) = delay.numer_denom_ms();
        let delay_ms = if denom > 0 && numer > 0 {
            ((numer as f32 / denom as f32).max(20.0).min(1000.0)) as u32
        } else {
            100
        };
        
        let img = DynamicImage::ImageRgba8(frame.buffer().clone());
        let scaled = scale_to_square(img, 72);
        
        let mut png_bytes = Vec::new();
        scaled.write_to(&mut std::io::Cursor::new(&mut png_bytes), ImageFormat::Png)
            .map_err(|e| format!("PNG encode failed: {}", e))?;
        
        let frame_data = format!("data:image/png;base64,{}", general_purpose::STANDARD.encode(&png_bytes));
        processed_frames.push((frame_data, delay_ms));
    }
    
    Ok(processed_frames)
}

fn decode_data_url(data_url: &str) -> Result<Vec<u8>, String> {
    let parts: Vec<&str> = data_url.split(',').collect();
    if parts.len() != 2 {
        return Err("Invalid data URL format".to_string());
    }
    general_purpose::STANDARD.decode(parts[1])
        .map_err(|e| format!("Base64 decode error: {}", e))
}

fn is_gif(data_url: &str) -> bool {
    data_url.contains("image/gif;base64") || data_url.starts_with("data:image/gif")
}

fn process_image(image_data: &str) -> Result<String, String> {
    let image_bytes = decode_data_url(image_data)?;
    if image_bytes.len() > 10 * 1024 * 1024 {
        return Err("Image exceeds 10MB size limit".to_string());
    }
    
    let img = image::load_from_memory(&image_bytes)
        .map_err(|e| format!("Image load failed: {}", e))?;
    let scaled = scale_to_square(img, 72);
    
    let mut png_bytes = Vec::new();
    scaled.write_to(&mut std::io::Cursor::new(&mut png_bytes), ImageFormat::Png)
        .map_err(|e| format!("PNG encode failed: {}", e))?;
    
    Ok(format!("data:image/png;base64,{}", general_purpose::STANDARD.encode(&png_bytes)))
}

fn scale_to_square(img: DynamicImage, size: u32) -> DynamicImage {
    let (width, height) = img.dimensions();
    if width == 0 || height == 0 {
        return img;
    }
    
    let min_dim = width.min(height);
    let scale = size as f32 / min_dim as f32;
    let new_width = (width as f32 * scale).round() as u32;
    let new_height = (height as f32 * scale).round() as u32;
    
    let resized = img.resize_exact(new_width, new_height, image::imageops::FilterType::Lanczos3);
    
    if new_width > size || new_height > size {
        let x = (new_width.saturating_sub(size)) / 2;
        let y = (new_height.saturating_sub(size)) / 2;
        resized.crop_imm(x, y, size, size)
    } else {
        resized
    }
}