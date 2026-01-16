An end-to-end data pipeline for building a Question-Answering system using Retrieval-Augmented Generation (RAG) with Large Language Models. The pipeline ingests, processes, and indexes Q&A data from Reddit and Stack Overflow to power intelligent question-answering capabilities.

This project implements a complete data pipeline that:

Ingests Q&A data from multiple sources (Reddit, Stack Overflow)
Cleans and processes the raw data using Apache Spark
Merges posts with their comments to create comprehensive Q&A pairs
Chunks the data using contextual chunking strategies
Generates embeddings and stores them for retrieval
Powers a RAG application that answers questions using retrieved context

#Features

Multi-Source Ingestion: Automated data collection from Reddit and Stack Overflow APIs
Distributed Processing: Apache Spark for scalable data cleaning and transformation
Contextual Chunking: Smart text segmentation that preserves semantic meaning
Object Storage: MinIO integration for distributed data storage
RAG System: Retrieval-Augmented Generation for accurate question answering
Docker Support: Containerized deployment with Docker Compose
Modular Pipeline: Each stage can be run independently or as a complete workflow


Data Pipeline Workflow

Stage 1: Ingestion

Connects to Reddit and Stack Overflow APIs
Fetches posts and comments based on specified criteria
Stores raw data in MinIO

Stage 2: Cleaning

Removes duplicates and invalid entries
Normalizes text format
Filters low-quality content
Handles missing data

Stage 3: Joining

Combines posts with their associated comments
Creates comprehensive Q&A pairs
Maintains relationships between questions and answers

Stage 4: Merging

Consolidates data from multiple platforms
Standardizes schema across sources
Deduplicates cross-platform content

Stage 5: Chunking

Applies contextual chunking strategies
Preserves semantic meaning in text segments
Optimizes chunk size for retrieval

Stage 6: LLM Processing

Generates embeddings for all chunks
Prepares data for vector storage
Creates metadata for enhanced retrieval

Stage 7: RAG Application

Retrieves relevant context based on user queries
Augments LLM prompts with retrieved information
Generates accurate, context-aware answers
