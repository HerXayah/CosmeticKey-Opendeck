use openaction::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use base64::{Engine as _, engine::general_purpose};
use image::{AnimationDecoder, DynamicImage, ImageFormat, GenericImageView};
use std::time::Duration;

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct CosmeticKeySettings {
	image_data: Option<String>,
}

pub struct CosmeticKeyAction;

// Global map to track animation stop channels by context
lazy_static::lazy_static! {
	static ref ANIMATIONS: Arc<RwLock<HashMap<String, tokio::sync::oneshot::Sender<()>>>> 
		= Arc::new(RwLock::new(HashMap::new()));
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
		
		if let Some(image_data) = &settings.image_data {
			if is_gif(image_data) {
				stop_animation(&instance_id).await;
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
		log::debug!("will_disappear called for instance {}", instance.instance_id);
		let instance_id = instance.instance_id.clone();
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

async fn stop_animation(instance_id: &str) {
	let mut animations = ANIMATIONS.write().await;
	if let Some(stop_tx) = animations.remove(instance_id) {
		log::info!("Stopping animation for instance {}", instance_id);
		drop(animations); // Drop lock before sending signal
		let _ = stop_tx.send(());
	} else {
		log::debug!("No animation to stop for instance {}", instance_id);
	}
}

async fn retry_get_instance(instance_id: String, max_attempts: u32, delay_ms: u64) -> Option<Arc<Instance>> {
	for attempt in 0..max_attempts {
		if let Some(instance) = get_instance(instance_id.clone()).await {
			if attempt > 0 {
				log::debug!("Got instance {} after {} attempts", instance_id, attempt + 1);
			}
			return Some(instance);
		}
		tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
	}
	None
}

async fn start_animation(instance_id: String, image_data: String) -> OpenActionResult<()> {
	let frames = match tokio::task::spawn_blocking(move || prepare_gif_frames(&image_data)).await {
		Ok(Ok(frames)) => frames,
		_ => {
			log::error!("Failed to prepare GIF frames for instance {}", instance_id);
			return Ok(());
		}
	};
	
	if frames.is_empty() {
		log::warn!("No frames in GIF for instance {}", instance_id);
		return Ok(());
	}
	
	let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
	
	let instance_id_for_task = instance_id.clone();
	let animations_ref = ANIMATIONS.clone();
	
	tokio::spawn(async move {
		// Try to get instance with retries (15 attempts × 100ms = 1.5s max)
		let Some(mut cached_instance) = retry_get_instance(instance_id_for_task.clone(), 15, 100).await else {
			log::warn!("Failed to get instance {} for animation", instance_id_for_task);
			animations_ref.write().await.remove(&instance_id_for_task);
			return;
		};
		
		// Pre-calculate cumulative timing for each frame
		let mut frame_timings = Vec::with_capacity(frames.len());
		let mut cumulative_ms = 0u64;
		for (_, delay_ms) in &frames {
			frame_timings.push(cumulative_ms);
			cumulative_ms += *delay_ms as u64;
		}
		let total_duration_ms = cumulative_ms;
		
		// Start timing AFTER we have successfully retrieved the instance
		let animation_start = tokio::time::Instant::now();
		let mut stop_rx = stop_rx;
		
		loop {
			// Calculate which frame should be showing based on elapsed time
			let elapsed = animation_start.elapsed().as_millis() as u64;
			let position_in_loop = elapsed % total_duration_ms;
			
			// Find the current frame
			let frame_index = frame_timings.iter()
				.position(|&t| t > position_in_loop)
				.map(|i| i.saturating_sub(1))
				.unwrap_or(frames.len() - 1);
			
			let (frame_data, _) = &frames[frame_index];
			
			// Display the frame
			if let Err(e) = cached_instance.set_image(Some(frame_data.clone()), None).await {
				log::warn!("Failed to set image for instance {}: {:?}", instance_id_for_task, e);
				// If image setting fails, try to get fresh instance
				if let Some(new_instance) = get_instance(instance_id_for_task.clone()).await {
					cached_instance = new_instance;
				} else {
					log::error!("Lost instance {}, stopping animation", instance_id_for_task);
					break;
				}
			}
			
			// Calculate when the next frame should appear
			let next_frame_time = frame_timings.get(frame_index + 1)
				.copied()
				.unwrap_or(total_duration_ms);
			let time_until_next = next_frame_time.saturating_sub(position_in_loop);
			
			// Wait until next frame or stop signal
			let wait_duration = Duration::from_millis(time_until_next.max(1));
			
			tokio::select! {
				_ = tokio::time::sleep(wait_duration) => {},
				_ = &mut stop_rx => {
					log::debug!("Animation stopped for instance {}", instance_id_for_task);
					break;
				}
			}
		}
		
		animations_ref.write().await.remove(&instance_id_for_task);
	});
	
	ANIMATIONS.write().await.insert(instance_id, stop_tx);
	Ok(())
}

fn prepare_gif_frames(image_data: &str) -> Result<Vec<(String, u32)>, String> {
	let gif_bytes = decode_data_url(image_data)?;
	
	let decoder = image::codecs::gif::GifDecoder::new(std::io::Cursor::new(&gif_bytes))
		.map_err(|e| format!("Failed to decode GIF: {}", e))?;
	
	let frames = decoder.into_frames().collect_frames()
		.map_err(|e| format!("Failed to extract frames: {}", e))?;
	
	if frames.is_empty() {
		return Err("GIF has no frames".to_string());
	}
	
	let mut processed_frames = Vec::with_capacity(frames.len());
	
	for frame in frames {
		let delay = frame.delay();
		let (numer, denom) = delay.numer_denom_ms();
		// Calculate actual delay: numerator / denominator, with minimum 20ms
		let delay_ms = if denom > 0 && numer > 0 {
			((numer as f32 / denom as f32).max(20.0)) as u32
		} else {
			100
		};
		
		let img = DynamicImage::ImageRgba8(frame.buffer().clone());
		let scaled = scale_to_square(img, 72);
		
		let mut png_bytes = Vec::new();
		scaled.write_to(&mut std::io::Cursor::new(&mut png_bytes), ImageFormat::Png)
			.map_err(|e| format!("Failed to encode PNG: {}", e))?;
		
		let frame_data = format!("data:image/png;base64,{}", general_purpose::STANDARD.encode(&png_bytes));
		processed_frames.push((frame_data, delay_ms));
	}
	
	Ok(processed_frames)
}

fn decode_data_url(data_url: &str) -> Result<Vec<u8>, String> {
	// Extract base64 part from data URL (format: data:image/gif;base64,...)
	let parts: Vec<&str> = data_url.split(',').collect();
	if parts.len() != 2 {
		return Err("Invalid data URL format".to_string());
	}
	
	general_purpose::STANDARD.decode(parts[1])
		.map_err(|e| format!("Base64 decode error: {}", e))
}

fn is_gif(data_url: &str) -> bool {
	data_url.contains("image/gif")
}

fn process_image(image_data: &str) -> Result<String, String> {
	// Decode base64
	let image_bytes = decode_data_url(image_data)?;
	
	// Load image
	let img = image::load_from_memory(&image_bytes)
		.map_err(|e| format!("Failed to load image: {}", e))?;
	
	// Scale to button size (72x72) with cover behavior
	let scaled = scale_to_square(img, 72);
	
	// Encode back to PNG
	let mut png_bytes: Vec<u8> = Vec::new();
	scaled.write_to(&mut std::io::Cursor::new(&mut png_bytes), ImageFormat::Png)
		.map_err(|e| format!("Failed to encode PNG: {}", e))?;
	
	Ok(format!("data:image/png;base64,{}", general_purpose::STANDARD.encode(&png_bytes)))
}

fn scale_to_square(img: DynamicImage, size: u32) -> DynamicImage {
	let (width, height) = img.dimensions();
	
	let min_dim = width.min(height);
	if min_dim == 0 {
		return img;
	}
	
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
