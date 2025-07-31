use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::io::{Read, Write, Result as IoResult};

use crate::encoder::{EncodedFrame, EncodedAudioFrame, FrameCallback};

/// Trait for reading from a stream
pub trait StreamReader: Send + Sync {
    /// Read data from the stream
    fn read(&mut self, buffer: &mut [u8]) -> IoResult<usize>;
    
    /// Check if the stream is still connected/available
    fn is_connected(&self) -> bool;
    
    /// Get the stream identifier for logging/debugging
    fn stream_id(&self) -> &str;
}

/// Trait for writing to a stream
pub trait StreamWriter: Send + Sync {
    /// Write data to the stream
    fn write(&mut self, data: &[u8]) -> IoResult<usize>;
    
    /// Flush any buffered data
    fn flush(&mut self) -> IoResult<()>;
    
    /// Check if the stream is still connected/available
    fn is_connected(&self) -> bool;
    
    /// Get the stream identifier for logging/debugging
    fn stream_id(&self) -> &str;
}

/// Trait for managing stream connections
pub trait StreamManager: Send + Sync {
    /// Accept a new connection and return a reader/writer pair
    fn accept_connection(&mut self) -> IoResult<(Box<dyn StreamReader>, Box<dyn StreamWriter>)>;
    
    /// Close the stream manager and cleanup resources
    fn close(&mut self) -> IoResult<()>;
    
    /// Get the manager identifier for logging/debugging
    fn manager_id(&self) -> &str;
}

/// TCP stream reader implementation
pub struct TcpStreamReader {
    stream: TcpStream,
    id: String,
}

impl TcpStreamReader {
    pub fn new(stream: TcpStream) -> Self {
        let id = format!("tcp-reader-{}", stream.peer_addr().unwrap_or_default());
        Self { stream, id }
    }
}

impl StreamReader for TcpStreamReader {
    fn read(&mut self, buffer: &mut [u8]) -> IoResult<usize> {
        self.stream.read(buffer)
    }
    
    fn is_connected(&self) -> bool {
        // Check if the stream is still valid by trying to get peer address
        self.stream.peer_addr().is_ok()
    }
    
    fn stream_id(&self) -> &str {
        &self.id
    }
}

/// TCP stream writer implementation
pub struct TcpStreamWriter {
    stream: TcpStream,
    id: String,
}

impl TcpStreamWriter {
    pub fn new(stream: TcpStream) -> Self {
        let id = format!("tcp-writer-{}", stream.peer_addr().unwrap_or_default());
        Self { stream, id }
    }
}

impl StreamWriter for TcpStreamWriter {
    fn write(&mut self, data: &[u8]) -> IoResult<usize> {
        self.stream.write(data)
    }
    
    fn flush(&mut self) -> IoResult<()> {
        self.stream.flush()
    }
    
    fn is_connected(&self) -> bool {
        self.stream.peer_addr().is_ok()
    }
    
    fn stream_id(&self) -> &str {
        &self.id
    }
}

/// TCP stream manager implementation
pub struct TcpStreamManager {
    listener: TcpListener,
    address: String,
}

impl TcpStreamManager {
    pub fn new(address: String) -> IoResult<Self> {
        let listener = TcpListener::bind(&address)?;
        println!("TCP stream manager started on {}", address);
        Ok(Self { listener, address })
    }
}

impl StreamManager for TcpStreamManager {
    fn accept_connection(&mut self) -> IoResult<(Box<dyn StreamReader>, Box<dyn StreamWriter>)> {
        let (stream, addr) = self.listener.accept()?;
        println!("New TCP connection from {}", addr);
        
        let reader = Box::new(TcpStreamReader::new(stream.try_clone()?));
        let writer = Box::new(TcpStreamWriter::new(stream));
        
        Ok((reader, writer))
    }
    
    fn close(&mut self) -> IoResult<()> {
        // TCP listener will be dropped automatically
        println!("TCP stream manager on {} closed", self.address);
        Ok(())
    }
    
    fn manager_id(&self) -> &str {
        &self.address
    }
}

/// UDP stream reader implementation
pub struct UdpStreamReader {
    socket: UdpSocket,
    id: String,
    buffer: Vec<u8>,
}

impl UdpStreamReader {
    pub fn new(socket: UdpSocket) -> Self {
        let id = format!("udp-reader-{}", socket.local_addr().unwrap_or_default());
        Self { 
            socket, 
            id,
            buffer: Vec::new(),
        }
    }
}

impl StreamReader for UdpStreamReader {
    fn read(&mut self, buffer: &mut [u8]) -> IoResult<usize> {
        // For UDP, we need to handle the fact that we might receive more data than requested
        if self.buffer.is_empty() {
            let mut temp_buffer = [0u8; 65536]; // Max UDP packet size
            let bytes_read = self.socket.recv(&mut temp_buffer)?;
            self.buffer.extend_from_slice(&temp_buffer[..bytes_read]);
        }
        
        let bytes_to_copy = std::cmp::min(buffer.len(), self.buffer.len());
        buffer[..bytes_to_copy].copy_from_slice(&self.buffer[..bytes_to_copy]);
        self.buffer.drain(..bytes_to_copy);
        
        Ok(bytes_to_copy)
    }
    
    fn is_connected(&self) -> bool {
        self.socket.local_addr().is_ok()
    }
    
    fn stream_id(&self) -> &str {
        &self.id
    }
}

/// UDP stream writer implementation
pub struct UdpStreamWriter {
    socket: UdpSocket,
    target_addr: String,
    id: String,
}

impl UdpStreamWriter {
    pub fn new(socket: UdpSocket, target_addr: String) -> Self {
        let id = format!("udp-writer-{}", socket.local_addr().unwrap_or_default());
        Self { socket, target_addr, id }
    }
}

impl StreamWriter for UdpStreamWriter {
    fn write(&mut self, data: &[u8]) -> IoResult<usize> {
        // For UDP, we send the entire data as one packet
        let bytes_sent = self.socket.send(data)?;
        Ok(bytes_sent)
    }
    
    fn flush(&mut self) -> IoResult<()> {
        // UDP doesn't need flushing
        Ok(())
    }
    
    fn is_connected(&self) -> bool {
        self.socket.local_addr().is_ok()
    }
    
    fn stream_id(&self) -> &str {
        &self.id
    }
}

/// UDP stream manager implementation
pub struct UdpStreamManager {
    socket: UdpSocket,
    address: String,
}

impl UdpStreamManager {
    pub fn new(address: String) -> IoResult<Self> {
        let socket = UdpSocket::bind(&address)?;
        println!("UDP stream manager started on {}", address);
        Ok(Self { socket, address })
    }
}

impl StreamManager for UdpStreamManager {
    fn accept_connection(&mut self) -> IoResult<(Box<dyn StreamReader>, Box<dyn StreamWriter>)> {
        // For UDP, we create reader/writer pairs that share the same socket
        let reader = Box::new(UdpStreamReader::new(self.socket.try_clone()?));
        let writer = Box::new(UdpStreamWriter::new(self.socket.try_clone()?, self.address.clone()));
        
        Ok((reader, writer))
    }
    
    fn close(&mut self) -> IoResult<()> {
        println!("UDP stream manager on {} closed", self.address);
        Ok(())
    }
    
    fn manager_id(&self) -> &str {
        &self.address
    }
}

/// Enhanced network callback that uses the trait system
pub struct TraitBasedNetworkCallback {
    stream_manager: Box<dyn StreamManager>,
    writers: Vec<Box<dyn StreamWriter>>,
    config: NetworkConfig,
}

impl TraitBasedNetworkCallback {
    pub fn new(stream_manager: Box<dyn StreamManager>, config: NetworkConfig) -> Self {
        Self {
            stream_manager,
            writers: Vec::new(),
            config,
        }
    }
    
    /// Accept new connections in a background thread
    pub fn start_accepting_connections(&mut self) -> IoResult<()> {
        let manager = self.stream_manager.as_mut();
        let writers = &mut self.writers;
        
        thread::spawn(move || {
            loop {
                match manager.accept_connection() {
                    Ok((_reader, writer)) => {
                        println!("New connection accepted: {}", writer.stream_id());
                        writers.push(writer);
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                        break;
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Broadcast a frame to all connected writers
    pub fn broadcast_frame(&mut self, frame: &EncodedFrame) -> IoResult<()> {
        let frame_data = self.serialize_frame(frame)?;
        
        // Remove disconnected writers
        self.writers.retain_mut(|writer| {
            if !writer.is_connected() {
                println!("Removing disconnected writer: {}", writer.stream_id());
                return false;
            }
            
            match writer.write(&frame_data) {
                Ok(_) => true,
                Err(e) => {
                    eprintln!("Failed to write to {}: {}", writer.stream_id(), e);
                    false
                }
            }
        });
        
        Ok(())
    }
    
    fn serialize_frame(&self, frame: &EncodedFrame) -> IoResult<Vec<u8>> {
        use std::io::{Cursor, Write};
        use byteorder::{LittleEndian, WriteBytesExt};

        let mut buffer = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);

        // Write frame header
        cursor.write_u32::<LittleEndian>(frame.data.len() as u32)?;
        cursor.write_i64::<LittleEndian>(frame.timestamp)?;
        cursor.write_u32::<LittleEndian>(frame.frame_type as u32)?;
        cursor.write_u32::<LittleEndian>(frame.width)?;
        cursor.write_u32::<LittleEndian>(frame.height)?;

        // Write frame data
        cursor.write_all(&frame.data)?;

        Ok(buffer)
    }
}

/// A network callback that implements FrameCallback for streaming
pub struct NetworkCallback {
    tcp_server: Option<TcpStreamServer>,
    udp_client: Option<UdpStreamClient>,
    config: NetworkConfig,
}

impl NetworkCallback {
    /// Creates a new network callback
    ///
    /// # Arguments
    ///
    /// * `config` - The network configuration
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the callback instance if successful
    pub fn new(config: NetworkConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let tcp_server = if config.protocol == Protocol::Tcp {
            Some(TcpStreamServer::new(config.clone())?)
        } else {
            None
        };

        let udp_client = if config.protocol == Protocol::Udp {
            Some(UdpStreamClient::new(config.clone())?)
        } else {
            None
        };

        Ok(Self {
            tcp_server,
            udp_client,
            config,
        })
    }

    /// Starts the network services
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref server) = self.tcp_server {
            server.start()?;
        }
        Ok(())
    }
}

impl FrameCallback for NetworkCallback {
    fn on_video_frame(&mut self, frame: EncodedFrame) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.config.protocol {
            Protocol::Tcp => {
                if let Some(ref server) = self.tcp_server {
                    server.broadcast_frame(frame)?;
                }
            }
            Protocol::Udp => {
                if let Some(ref mut client) = self.udp_client {
                    client.send_frame(frame)?;
                }
            }
            Protocol::WebRtc => {
                // WebRTC implementation would go here
                eprintln!("WebRTC protocol not yet implemented");
            }
            Protocol::Rtmp => {
                // RTMP implementation would go here
                eprintln!("RTMP protocol not yet implemented");
            }
        }
        Ok(())
    }

    fn on_audio_frame(&mut self, frame: EncodedAudioFrame) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Audio frame handling would go here
        // For now, we'll just log it
        println!("Received audio frame: {} bytes, timestamp: {}", frame.data.len(), frame.timestamp);
        Ok(())
    }

    fn on_stream_start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Network stream started");
        self.start()?;
        Ok(())
    }

    fn on_stream_end(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Network stream ended");
        Ok(())
    }
}

/// Enhanced network callback that uses the trait system
pub struct EnhancedNetworkCallback {
    trait_callback: TraitBasedNetworkCallback,
    config: NetworkConfig,
}

impl EnhancedNetworkCallback {
    /// Creates a new enhanced network callback using the trait system
    ///
    /// # Arguments
    ///
    /// * `config` - The network configuration
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the callback instance if successful
    pub fn new(config: NetworkConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream_manager: Box<dyn StreamManager> = match config.protocol {
            Protocol::Tcp => {
                Box::new(TcpStreamManager::new(config.address.clone())?)
            }
            Protocol::Udp => {
                Box::new(UdpStreamManager::new(config.address.clone())?)
            }
            _ => {
                return Err("Unsupported protocol for trait-based streaming".into());
            }
        };

        let trait_callback = TraitBasedNetworkCallback::new(stream_manager, config.clone());
        
        Ok(Self {
            trait_callback,
            config,
        })
    }

    /// Starts accepting connections using the trait system
    pub fn start_accepting_connections(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.trait_callback.start_accepting_connections()?;
        Ok(())
    }
}

impl FrameCallback for EnhancedNetworkCallback {
    fn on_video_frame(&mut self, frame: EncodedFrame) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.trait_callback.broadcast_frame(&frame)?;
        Ok(())
    }

    fn on_audio_frame(&mut self, frame: EncodedAudioFrame) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Enhanced callback received audio frame: {} bytes, timestamp: {}", frame.data.len(), frame.timestamp);
        Ok(())
    }

    fn on_stream_start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Enhanced network stream started using trait system");
        self.start_accepting_connections()?;
        Ok(())
    }

    fn on_stream_end(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Enhanced network stream ended");
        Ok(())
    }
}