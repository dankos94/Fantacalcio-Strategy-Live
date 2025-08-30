package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Stagione;
import it.costantino.astaapp.repository.StagioneRepository;
import it.costantino.astaapp.service.StagioneService;
import it.costantino.astaapp.service.dto.StagioneDTO;
import it.costantino.astaapp.service.mapper.StagioneMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Stagione}.
 */
@Service
@Transactional
public class StagioneServiceImpl implements StagioneService {

    private static final Logger LOG = LoggerFactory.getLogger(StagioneServiceImpl.class);

    private final StagioneRepository stagioneRepository;

    private final StagioneMapper stagioneMapper;

    public StagioneServiceImpl(StagioneRepository stagioneRepository, StagioneMapper stagioneMapper) {
        this.stagioneRepository = stagioneRepository;
        this.stagioneMapper = stagioneMapper;
    }

    @Override
    public StagioneDTO save(StagioneDTO stagioneDTO) {
        LOG.debug("Request to save Stagione : {}", stagioneDTO);
        Stagione stagione = stagioneMapper.toEntity(stagioneDTO);
        stagione = stagioneRepository.save(stagione);
        return stagioneMapper.toDto(stagione);
    }

    @Override
    public StagioneDTO update(StagioneDTO stagioneDTO) {
        LOG.debug("Request to update Stagione : {}", stagioneDTO);
        Stagione stagione = stagioneMapper.toEntity(stagioneDTO);
        stagione = stagioneRepository.save(stagione);
        return stagioneMapper.toDto(stagione);
    }

    @Override
    public Optional<StagioneDTO> partialUpdate(StagioneDTO stagioneDTO) {
        LOG.debug("Request to partially update Stagione : {}", stagioneDTO);

        return stagioneRepository
            .findById(stagioneDTO.getId())
            .map(existingStagione -> {
                stagioneMapper.partialUpdate(existingStagione, stagioneDTO);

                return existingStagione;
            })
            .map(stagioneRepository::save)
            .map(stagioneMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<StagioneDTO> findAll() {
        LOG.debug("Request to get all Stagiones");
        return stagioneRepository.findAll().stream().map(stagioneMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<StagioneDTO> findOne(Long id) {
        LOG.debug("Request to get Stagione : {}", id);
        return stagioneRepository.findById(id).map(stagioneMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Stagione : {}", id);
        stagioneRepository.deleteById(id);
    }
}
